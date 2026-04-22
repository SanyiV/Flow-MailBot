import io
import json
import imaplib
import email
import os
import atexit
from pathlib import Path
import re
import time
import smtplib
import tomllib
from email.header import decode_header
from email.utils import formatdate, make_msgid, parseaddr
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse

import pandas as pd
from openai import OpenAI

MODEL_NAME = "gpt-4o"
APP_ROOT = Path(__file__).resolve().parent
SAVED_KB_XLSX_PATH = APP_ROOT / "saved_knowledge_base.xlsx"
SECRETS_TOML_PATH = APP_ROOT / "hotel_credentials.toml"
AUTOMATION_STATUS_PATH = APP_ROOT / "automation_status.txt"
PID_LOCK_PATH = APP_ROOT / "motor.pid"
IMAP_SERVER_DEFAULT = "imap.gmail.com"
PRIMARY_LANGUAGE_DEFAULT = "English"
SPOKEN_LANGUAGES_DEFAULT_LIST = ["English", "Hungarian"]
POLL_INTERVAL_SECONDS = 30
IMAP_TIMEOUT_SECONDS = 10


def _normalize_imap_server_address(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return IMAP_SERVER_DEFAULT
    if "imap.gmail.com" in s.lower():
        return "imap.gmail.com"
    low = s.lower()
    if low.startswith("imap://"):
        s = s[7:].strip()
    elif low.startswith("ssl://"):
        s = s[6:].strip()
    elif low.startswith("https://") or low.startswith("http://"):
        try:
            host = (urlparse(s).hostname or "").strip()
            if host:
                s = host
        except Exception:
            pass
    s = s.strip().strip("/")
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    return s or IMAP_SERVER_DEFAULT


def _load_dotenv_if_present(dotenv_path: Path) -> None:
    try:
        if not dotenv_path.exists():
            return
        for raw in dotenv_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
    except Exception:
        return


_load_dotenv_if_present(APP_ROOT / ".env")


def _read_credentials_toml() -> dict:
    if not SECRETS_TOML_PATH.exists():
        return {}
    try:
        data = tomllib.loads(SECRETS_TOML_PATH.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _get_config_value(*keys: str, default: str = "") -> str:
    cfg = _read_credentials_toml()
    for k in keys:
        v = str(cfg.get(k) or "").strip()
        if v:
            return v
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return default


def _get_config_list(*keys: str) -> list[str]:
    def _to_list(value: object) -> list[str]:
        if isinstance(value, list):
            return [str(x).strip() for x in value if str(x).strip()]
        if isinstance(value, tuple):
            return [str(x).strip() for x in value if str(x).strip()]
        if isinstance(value, str):
            return [p.strip() for p in value.split(",") if p.strip()]
        return []

    cfg = _read_credentials_toml()
    for k in keys:
        vals = _to_list(cfg.get(k))
        if vals:
            return vals
    for k in keys:
        vals = _to_list(os.getenv(k))
        if vals:
            return vals
    return []


def _normalize_spoken_languages_for_prompt(spoken_languages: str | list[str] | tuple[str, ...] | None) -> str:
    if isinstance(spoken_languages, (list, tuple)):
        cleaned = [str(x).strip() for x in spoken_languages if str(x).strip()]
        return ", ".join(cleaned)
    return str(spoken_languages or "").strip()


def _read_automation_status_flag() -> str:
    if not AUTOMATION_STATUS_PATH.exists():
        return "STOPPED"
    try:
        raw = (AUTOMATION_STATUS_PATH.read_text(encoding="utf-8", errors="ignore") or "").strip().upper()
    except Exception:
        return "STOPPED"
    return raw if raw in {"RUNNING", "STOPPED"} else "STOPPED"


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _release_pid_lock() -> None:
    try:
        if PID_LOCK_PATH.exists():
            current = int((PID_LOCK_PATH.read_text(encoding="utf-8", errors="ignore") or "0").strip() or "0")
            if current == os.getpid():
                PID_LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        return


def _acquire_pid_lock() -> None:
    if PID_LOCK_PATH.exists():
        try:
            existing_pid = int((PID_LOCK_PATH.read_text(encoding="utf-8", errors="ignore") or "0").strip() or "0")
        except Exception:
            existing_pid = 0
        if existing_pid and _pid_is_alive(existing_pid):
            raise RuntimeError(f"Another motor.py instance is already running (pid={existing_pid}).")
        try:
            PID_LOCK_PATH.unlink(missing_ok=True)
        except Exception:
            pass
    PID_LOCK_PATH.write_text(str(os.getpid()), encoding="utf-8")
    atexit.register(_release_pid_lock)


def decode_mime_words(value: str) -> str:
    if not value:
        return ""
    try:
        parts = decode_header(value)
        out: list[str] = []
        for part, enc in parts:
            if isinstance(part, bytes):
                out.append(part.decode(enc or "utf-8", errors="replace"))
            else:
                out.append(str(part))
        return "".join(out).strip()
    except Exception:
        return str(value).strip()


def _read_knowledge_base_from_bytes(filename: str, file_bytes: bytes) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".csv"):
        raw = file_bytes
        for enc in ("utf-8", "utf-8-sig", "cp1250", "latin-1"):
            try:
                return pd.read_csv(io.BytesIO(raw), encoding=enc)
            except Exception:
                continue
        return pd.read_csv(io.BytesIO(raw))
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(file_bytes))
    raise ValueError("Unsupported file format.")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for c in df.columns:
        c_norm = str(c).strip().lower()
        if c_norm in {"kategoria", "kategória", "category", "categorie", "categoria"}:
            renamed[c] = "Category"
        elif c_norm in {"szabalyzat", "szabályzat", "policy", "rule", "szabaly", "szabály"}:
            renamed[c] = "Policy"
    if renamed:
        df = df.rename(columns=renamed)
    return df


def _load_kb_df_from_disk() -> pd.DataFrame | None:
    if not SAVED_KB_XLSX_PATH.exists():
        return None
    try:
        file_bytes = SAVED_KB_XLSX_PATH.read_bytes()
        return _normalize_columns(_read_knowledge_base_from_bytes(SAVED_KB_XLSX_PATH.name, file_bytes))
    except Exception as e:
        print(f"[KB] Failed to load KB from disk: {e}")
        return None


def _kb_rows_for_prompt(kb: pd.DataFrame, max_rows: int = 200) -> list[dict]:
    kb = _normalize_columns(kb)
    if "Category" not in kb.columns or "Policy" not in kb.columns:
        raise ValueError("Knowledge base must contain Category and Policy columns.")
    rows = []
    for _, r in kb[["Category", "Policy"]].dropna(how="all").head(max_rows).iterrows():
        cat = "" if pd.isna(r["Category"]) else str(r["Category"]).strip()
        pol = "" if pd.isna(r["Policy"]) else str(r["Policy"]).strip()
        if cat or pol:
            rows.append({"category": cat, "policy": pol})
    if not rows:
        raise ValueError("Knowledge base is empty.")
    return rows


def _get_openai_client() -> OpenAI:
    key = (_get_config_value("OPENAI_API_KEY", default="") or "").strip()
    if not key:
        raise ValueError("Missing OpenAI API key.")
    return OpenAI(api_key=key)


def _parse_json_object_maybe_fenced(text: str) -> dict:
    s = (text or "").strip()
    if not s:
        raise ValueError("Empty model response.")
    if s.startswith("```"):
        lines = s.splitlines()
        if lines and lines[0].lstrip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    s = s.strip().strip("`").strip()
    return json.loads(s)


def _select_best_policy(complaint: str, kb_rows: list[dict]) -> dict:
    client = _get_openai_client()
    system = (
        "You are a customer-support triage assistant. Select the single best policy entry.\n"
        "Return JSON only."
    )
    user = (
        "Incoming complaint:\n"
        f"{complaint}\n\n"
        "Knowledge base (JSON array):\n"
        f"{json.dumps(kb_rows, ensure_ascii=False)}\n"
    )
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        response_format={"type": "json_object"},
        temperature=0.1,
    )
    content = (resp.choices[0].message.content or "").strip()
    try:
        return _parse_json_object_maybe_fenced(content)
    except Exception as e:
        return {"match": False, "confidence": 0.0, "category": "", "policy": "", "reason": str(e)}


def _generate_reply_letter(complaint: str, selection: dict, primary_language: str, spoken_languages: str) -> str:
    complaint = (complaint or "").strip()
    if not complaint:
        return "Please paste the incoming complaint, then click Generate."
    complaint_for_output = re.sub(r"\r\n?", "\n", complaint).strip()
    complaint_for_output = re.sub(r"\n{2,}", "\n\n", complaint_for_output)

    primary_language = (primary_language or "").strip() or PRIMARY_LANGUAGE_DEFAULT
    spoken_languages_norm = _normalize_spoken_languages_for_prompt(spoken_languages)

    match = bool(selection.get("match"))
    confidence = float(selection.get("confidence") or 0.0)
    policy = (selection.get("policy") or "").strip()
    category = (selection.get("category") or "").strip()
    reason = (selection.get("reason") or "").strip()
    is_unknown_case = (not match) or confidence <= 0.4 or not policy
    case_name = "UNKNOWN" if is_unknown_case else "KNOWN"

    client = _get_openai_client()
    system = (
        "You are a strict email-draft formatter.\n"
        "First, detect the language of the incoming original customer email.\n"
        "CRITICAL RESPONSE-LANGUAGE RULE: line1 MUST be written in the detected incoming email language.\n"
        "Never answer the customer in Primary language unless the incoming email is actually in Primary language.\n"
        "Knowledge-base language is irrelevant for output language; it is only guidance for policy content.\n"
        f"CRITICAL RULE: If the incoming email language is NOT exactly '{primary_language}' and NOT in '{spoken_languages_norm or '(none)'}', you MUST append this strict template at the very end of your response:\n\n--- BELSŐ FORDÍTÁS ---\nEredeti üzenet: [YOU MUST STRICTLY TRANSLATE THE CUSTOMER'S ORIGINAL EMAIL TEXT INTO {primary_language}]\nVálasz: [YOU MUST STRICTLY TRANSLATE YOUR GENERATED RESPONSE INTO {primary_language}]\n\nNever leave the text in the original language inside the internal translation block, and never translate to English unless English is the {primary_language}.\n"
        "Return ONLY valid JSON with EXACTLY these keys: line1, internal_translation_block."
    )
    user = (
        f"Case type: {case_name}\n"
        f"Primary language: {primary_language}\n"
        f"Spoken languages: {spoken_languages_norm or '(none)'}\n"
        "Incoming complaint:\n"
        f"{complaint}\n\n"
        "Relevant knowledge-base entry:\n"
        f"Category: {category or '(none)'}\n"
        f"Policy: {policy or '(none)'}\n\n"
        "Selection rationale:\n"
        f"{reason or '(none)'}\n"
    )
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=0.2,
    )
    try:
        payload = _parse_json_object_maybe_fenced(resp.choices[0].message.content or "")
    except Exception:
        payload = {}
    line1 = re.sub(r"[\r\n]+", " ", str(payload.get("line1") or "")).strip()
    internal = str(payload.get("internal_translation_block") or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not line1:
        if is_unknown_case:
            line1 = "[ACTION REQUIRED] I could not process this email automatically."
        else:
            line1 = "Thank you for your message. We are reviewing your case based on our policy."
    if internal:
        return f"{line1}\n{internal}\n--- Original Message ---\n{complaint_for_output}"
    return f"{line1}\n--- Original Message ---\n{complaint_for_output}"


def _auto_clean_outgoing_body(text: str) -> str:
    if not text:
        return ""
    s = str(text).replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"(?m)^\[ACTION REQUIRED:[^\]]*\]\s*\n?", "", s).lstrip()
    return s.strip() + ("\n" if s.strip() else "")


def _build_reply_subject(original_subject: str) -> str:
    s = (original_subject or "").strip()
    if s.lower().startswith("re:"):
        return s
    return f"Re: {s}" if s else "Re:"


def fetch_email_by_uid(uid: str, source_mailbox: str = "INBOX") -> dict:
    uid_clean = (uid or "").strip()
    if not uid_clean:
        raise ValueError("Missing UID.")
    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(_get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT)), timeout=IMAP_TIMEOUT_SECONDS)
    try:
        mail.login(_get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default=""), _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default=""))
        mailbox = (source_mailbox or "INBOX").strip() or "INBOX"
        mail.select(mailbox)
        fstatus, fdata = mail.uid("fetch", uid_clean.encode("utf-8"), "(RFC822)")
        if fstatus != "OK" or not fdata or not fdata[0]:
            raise RuntimeError("Unable to fetch message by UID.")
        raw = fdata[0][1] or b""
        msg = email.message_from_bytes(raw)
        subject = decode_mime_words(msg.get("Subject", ""))
        sender = decode_mime_words(msg.get("From", ""))
        message_id = (msg.get("Message-ID") or "").strip()
        body_text = ""
        if msg.is_multipart():
            for part in msg.walk():
                ctype = part.get_content_type()
                disp = (part.get("Content-Disposition") or "").lower()
                if "attachment" in disp:
                    continue
                if ctype == "text/plain":
                    payload = part.get_payload(decode=True) or b""
                    charset = part.get_content_charset() or "utf-8"
                    body_text = payload.decode(charset, errors="replace").strip()
                    if body_text:
                        break
        else:
            payload = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            body_text = payload.decode(charset, errors="replace").strip()
        return {"uid": uid_clean, "source_mailbox": mailbox, "from": sender, "subject": subject, "message_id": message_id, "body": body_text}
    finally:
        try:
            mail.logout()
        except Exception:
            pass


def _discover_poll_mailboxes(mail: imaplib.IMAP4) -> list[str]:
    candidates = ["INBOX", "[Gmail]/Spam", "Spam", "Junk"]
    discovered: list[str] = []
    for mailbox in candidates:
        try:
            sel_status, _ = mail.select(mailbox)
            if (sel_status or "").upper() == "OK":
                discovered.append(mailbox)
        except Exception:
            continue
    out: list[str] = []
    for mb in discovered:
        if mb not in out:
            out.append(mb)
    return out or ["INBOX"]


def _search_unseen_uids() -> list[tuple[str, str]]:
    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(_get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT)), timeout=IMAP_TIMEOUT_SECONDS)
    try:
        mail.login(_get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default=""), _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default=""))
        mailboxes = _discover_poll_mailboxes(mail)
        refs: list[tuple[str, str]] = []
        seen_refs: set[tuple[str, str]] = set()
        for mailbox in mailboxes:
            sel_status, _ = mail.select(mailbox)
            if (sel_status or "").upper() != "OK":
                continue
            status, response = mail.search(None, "UNSEEN")
            if (status or "").upper() != "OK":
                continue
            seq_ids_b = [x for x in ((response[0] or b"").split()) if x]
            try:
                seq_ids_b = sorted(seq_ids_b, key=lambda b: int(b), reverse=True)
            except Exception:
                seq_ids_b = list(reversed(seq_ids_b))
            for seq_id in seq_ids_b:
                fstatus, fdata = mail.fetch(seq_id, "(UID)")
                if (fstatus or "").upper() != "OK" or not fdata or not fdata[0]:
                    continue
                raw_uid_hdr = fdata[0][0] if isinstance(fdata[0], tuple) else fdata[0]
                raw_uid_str = raw_uid_hdr.decode(errors="ignore") if isinstance(raw_uid_hdr, bytes) else str(raw_uid_hdr)
                m_uid = re.search(r"UID\s+(\d+)", raw_uid_str)
                if not m_uid:
                    continue
                uid_str = m_uid.group(1).strip()
                ref = (mailbox, uid_str)
                if ref in seen_refs:
                    continue
                seen_refs.add(ref)
                refs.append(ref)
        return refs
    finally:
        try:
            mail.logout()
        except Exception:
            pass


def _mark_seen(uid: str, source_mailbox: str = "INBOX", also_flag: bool = False) -> bool:
    uid_clean = (uid or "").strip()
    if not uid_clean:
        return False
    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(_get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT)))
    try:
        mail.login(_get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default=""), _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default=""))
        mail.select((source_mailbox or "INBOX").strip() or "INBOX")
        st_status, _ = mail.uid("store", uid_clean.encode("utf-8"), "+FLAGS", "\\Seen")
        if (st_status or "").upper() != "OK":
            return False
        if also_flag:
            try:
                mail.uid("store", uid_clean.encode("utf-8"), "+FLAGS", "\\Flagged")
            except Exception:
                pass
        return True
    finally:
        try:
            mail.logout()
        except Exception:
            pass


def _discover_gmail_drafts_mailbox(mail: imaplib.IMAP4) -> str:
    fallback_names = ["[Gmail]/Piszkozatok", "Piszkozatok", "[Gmail]/Drafts", "Drafts"]
    try:
        status, boxes = mail.list()
        if (status or "").upper() != "OK" or not boxes:
            return fallback_names[0]
    except Exception:
        return fallback_names[0]
    for raw in boxes:
        line = raw.decode(errors="ignore") if isinstance(raw, bytes) else str(raw)
        if "\\Drafts" in line or "\\DRAFTS" in line:
            m = re.search(r'"([^"]+)"\s*$', line.strip())
            if m:
                name = m.group(1).strip()
                if name and name.upper() != "INBOX":
                    return name
    return fallback_names[0]


def _save_reply_as_draft(uid: str, source_mailbox: str, to_address: str, reply_subject: str, clean_body: str, is_unique_case: bool) -> bool:
    if not (to_address or "").strip():
        return _mark_seen(uid, source_mailbox=source_mailbox, also_flag=is_unique_case)
    if not (clean_body or "").strip():
        return _mark_seen(uid, source_mailbox=source_mailbox, also_flag=is_unique_case)

    draft_user = _get_config_value("DRAFT_GMAIL_USER", "GMAIL_DRAFT_USER", default=_get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default=""))
    draft_pw = _get_config_value("DRAFT_GMAIL_PASSWORD", "GMAIL_DRAFT_PASSWORD", default=_get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default=""))

    msg = MIMEMultipart()
    msg["From"] = draft_user
    msg["To"] = to_address
    msg["Subject"] = reply_subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg.attach(MIMEText(clean_body, "plain", "utf-8"))
    msg_bytes = msg.as_string().encode("utf-8", errors="replace")

    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(_get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT)), timeout=IMAP_TIMEOUT_SECONDS)
    try:
        mail.login(draft_user, draft_pw)
        drafts_box = (_discover_gmail_drafts_mailbox(mail) or "").strip()
        if not drafts_box or drafts_box.upper() == "INBOX":
            return False
        ap_status, _ = mail.append(drafts_box, "(\\Draft \\Seen)", None, msg_bytes)
        if (ap_status or "").upper() != "OK":
            return False
    finally:
        try:
            mail.logout()
        except Exception:
            pass

    return _mark_seen(uid, source_mailbox=source_mailbox, also_flag=is_unique_case)


def _generate_reply_draft_text(complaint_text: str, kb_df: pd.DataFrame, primary_language: str, spoken_languages: str) -> tuple[str, bool]:
    complaint_clean = (complaint_text or "").strip()
    kb_rows = _kb_rows_for_prompt(kb_df)
    selection = _select_best_policy(complaint_clean, kb_rows)
    try:
        pol = (selection.get("policy") or "").strip()
        conf = float(selection.get("confidence") or 0.0)
        m = bool(selection.get("match"))
        is_unique_case = (not m) or conf <= 0.4 or (not pol)
    except Exception:
        is_unique_case = True
    draft = _generate_reply_letter(
        complaint_clean,
        selection,
        primary_language=(primary_language or PRIMARY_LANGUAGE_DEFAULT),
        spoken_languages=_normalize_spoken_languages_for_prompt(spoken_languages),
    )
    return draft, is_unique_case


def _automata_worker_loop() -> None:
    processed_email_ids: set[tuple[str, str]] = set()
    print("[Automata] Worker started.")
    while True:
        try:
            if _read_automation_status_flag() != "RUNNING":
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            email_address = _get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default="")
            email_pw = _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default="")
            openai_key = _get_config_value("OPENAI_API_KEY", default="")
            primary_language = (_get_config_value("PRIMARY_LANGUAGE", default=PRIMARY_LANGUAGE_DEFAULT) or PRIMARY_LANGUAGE_DEFAULT).strip()
            spoken_languages = _normalize_spoken_languages_for_prompt(_get_config_list("SPOKEN_LANGUAGES") or SPOKEN_LANGUAGES_DEFAULT_LIST)
            if not (email_address and email_pw and openai_key):
                print("[Automata] Missing credentials; sleeping.")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            kb_df = _load_kb_df_from_disk()
            if kb_df is None or kb_df.empty:
                print("[Automata] Knowledge base missing/empty; sleeping.")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            uid_refs = _search_unseen_uids()
            if not uid_refs:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            print(f"[Automata] UNSEEN found: {len(uid_refs)} message(s).")
            for source_mailbox, uid in uid_refs:
                email_ref = (source_mailbox, uid)
                if email_ref in processed_email_ids:
                    continue
                processed_email_ids.add(email_ref)
                try:
                    print(f"[Automata] Processing mailbox={source_mailbox} UID={uid} ...")
                    email_obj = fetch_email_by_uid(uid, source_mailbox=source_mailbox)
                    complaint_text = (email_obj.get("body") or "").strip()
                    if not complaint_text:
                        _mark_seen(uid, source_mailbox=source_mailbox, also_flag=False)
                        continue

                    draft_text, is_unique_case = _generate_reply_draft_text(
                        complaint_text,
                        kb_df,
                        primary_language=primary_language,
                        spoken_languages=spoken_languages,
                    )
                    clean_body = _auto_clean_outgoing_body(draft_text)
                    to_address = parseaddr(email_obj.get("from") or "")[1].strip()
                    reply_subject = _build_reply_subject(email_obj.get("subject") or "")

                    ok = _save_reply_as_draft(
                        uid=uid,
                        source_mailbox=source_mailbox,
                        to_address=to_address,
                        reply_subject=reply_subject,
                        clean_body=clean_body,
                        is_unique_case=is_unique_case,
                    )
                    if ok:
                        print(f"[Automata] Processed mailbox={source_mailbox} UID={uid} -> draft saved + marked Seen.")
                except Exception as e:
                    print(f"[Automata] Failed to process UID={uid}: {e}")
                    continue
        except Exception as e:
            print(f"[Automata] Worker loop error: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        _acquire_pid_lock()
    except Exception as e:
        print(f"[Automata] {e}")
        raise SystemExit(1)
    _automata_worker_loop()
