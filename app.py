import io
import json
import imaplib
import email
import os
from pathlib import Path
import re
import threading
import time
import smtplib
import tomllib
from email.header import decode_header
from email.message import EmailMessage
from email.utils import formatdate, make_msgid, parseaddr
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from openai import OpenAI

MODEL_NAME = "gpt-4o"

# Knowledge base persistence (saved to project root).
APP_ROOT = Path(__file__).resolve().parent
SAVED_KB_XLSX_PATH = APP_ROOT / "saved_knowledge_base.xlsx"
SAVED_KB_ORIG_NAME_PATH = APP_ROOT / "saved_filename.txt"
SECRETS_TOML_PATH = APP_ROOT / "hotel_credentials.toml"
AUTOMATION_STATUS_PATH = APP_ROOT / "automation_status.txt"

# Default IMAP host for Gmail; any provider can override via TOML/secrets/settings.
IMAP_SERVER_DEFAULT = "imap.gmail.com"


def _normalize_imap_server_address(raw: str) -> str:
    """
    Normalize user input into a hostname suitable for imaplib.IMAP4_SSL().
    Accepts plain hostnames (e.g. imap.gmail.com) or imap://host paths.
    """
    s = (raw or "").strip()
    if not s:
        return IMAP_SERVER_DEFAULT
    # Common mistaken paste: Google search URL pointing at imap.gmail.com — still use Gmail IMAP.
    if "imap.gmail.com" in s.lower():
        return "imap.gmail.com"
    low = s.lower()
    if low.startswith("imap://"):
        s = s[7:].strip()
    elif low.startswith("ssl://"):
        s = s[6:].strip()
    elif low.startswith("https://") or low.startswith("http://"):
        try:
            netloc = (urlparse(s).hostname or "").strip()
            if netloc:
                s = netloc
        except Exception:
            pass
    s = s.strip().strip("/")
    # Strip accidental path segments (host only).
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    return s or IMAP_SERVER_DEFAULT


def _load_dotenv_if_present(dotenv_path: Path) -> None:
    """
    Minimal .env loader (no external dependency).
    Loads KEY=VALUE pairs into os.environ if not already set.
    """
    try:
        if not dotenv_path.exists():
            return
        for raw_line in dotenv_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
    except Exception:
        # Best-effort only. Credentials can still come from system env or Streamlit secrets.
        return


_load_dotenv_if_present(APP_ROOT / ".env")


def _get_streamlit_secret(key: str) -> str | None:
    """
    Read a key from st.secrets without crashing if secrets.toml is missing.
    """
    try:
        val = st.secrets.get(key)  # type: ignore[attr-defined]
    except Exception:
        return None
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _get_streamlit_secret_raw(key: str):
    try:
        return st.secrets.get(key)  # type: ignore[attr-defined]
    except Exception:
        return None


def _read_managed_credentials_toml() -> dict:
    """
    Read credentials from hotel_credentials.toml in the project root.
    """
    if not SECRETS_TOML_PATH.exists():
        return {}
    try:
        data = tomllib.loads(SECRETS_TOML_PATH.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _get_config_value(*keys: str, default: str = "") -> str:
    """
    Resolve config from managed TOML, then Streamlit secrets, then env, then default.
    Accepts multiple possible key aliases.
    """
    managed = _read_managed_credentials_toml()
    for k in keys:
        v0 = str(managed.get(k) or "").strip()
        if v0:
            return v0
    for k in keys:
        v2 = _get_streamlit_secret(k)
        if v2:
            return v2
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return default


def _get_config_list(*keys: str) -> list[str]:
    def _coerce_list(value: object) -> list[str]:
        if isinstance(value, list):
            return [str(x).strip() for x in value if str(x).strip()]
        if isinstance(value, tuple):
            return [str(x).strip() for x in value if str(x).strip()]
        if isinstance(value, str):
            return [p.strip() for p in value.split(",") if p.strip()]
        return []

    managed = _read_managed_credentials_toml()
    for k in keys:
        if k in managed:
            vals = _coerce_list(managed.get(k))
            if vals:
                return vals
    for k in keys:
        raw = _get_streamlit_secret_raw(k)
        vals = _coerce_list(raw)
        if vals:
            return vals
    for k in keys:
        env_raw = os.getenv(k)
        vals = _coerce_list(env_raw)
        if vals:
            return vals
    return []


def _get_persisted_primary_language() -> str:
    candidate = (_get_config_value("PRIMARY_LANGUAGE", default=PRIMARY_LANGUAGE_DEFAULT) or "").strip()
    if candidate in LANGUAGE_OPTIONS:
        return candidate
    return PRIMARY_LANGUAGE_DEFAULT


def _get_persisted_spoken_languages() -> list[str]:
    configured = [lang for lang in _get_config_list("SPOKEN_LANGUAGES") if lang in LANGUAGE_OPTIONS]
    if configured:
        return configured
    fallback = [lang for lang in SPOKEN_LANGUAGES_DEFAULT_LIST if lang in LANGUAGE_OPTIONS]
    return fallback or [PRIMARY_LANGUAGE_DEFAULT]


OPENAI_API_KEY = _get_config_value("OPENAI_API_KEY", default="")
IMAP_SERVER_ADDRESS = _normalize_imap_server_address(
    _get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT)
)
EMAIL_ADDRESS = _get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default="")
EMAIL_APP_PASSWORD = _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default="")

# SMTP credentials (aliases for clarity / external references).
GMAIL_USER = EMAIL_ADDRESS
GMAIL_PASSWORD = EMAIL_APP_PASSWORD

# Optional: store drafts in a different mailbox (e.g. wife's account).
# If not provided, defaults to the same account we read the inbound emails from.
DRAFT_EMAIL_ADDRESS = _get_config_value(
    "DRAFT_GMAIL_USER",
    "GMAIL_DRAFT_USER",
    default=EMAIL_ADDRESS,
)
DRAFT_EMAIL_APP_PASSWORD = _get_config_value(
    "DRAFT_GMAIL_PASSWORD",
    "GMAIL_DRAFT_PASSWORD",
    default=EMAIL_APP_PASSWORD,
)


def _reload_runtime_credentials() -> None:
    """Re-read credentials from managed TOML / Streamlit secrets / env."""
    global OPENAI_API_KEY, IMAP_SERVER_ADDRESS, EMAIL_ADDRESS, EMAIL_APP_PASSWORD, GMAIL_USER, GMAIL_PASSWORD, DRAFT_EMAIL_ADDRESS, DRAFT_EMAIL_APP_PASSWORD
    OPENAI_API_KEY = _get_config_value("OPENAI_API_KEY", default="")
    IMAP_SERVER_ADDRESS = _normalize_imap_server_address(
        _get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT)
    )
    EMAIL_ADDRESS = _get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default="")
    EMAIL_APP_PASSWORD = _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default="")
    GMAIL_USER = EMAIL_ADDRESS
    GMAIL_PASSWORD = EMAIL_APP_PASSWORD
    DRAFT_EMAIL_ADDRESS = _get_config_value(
        "DRAFT_GMAIL_USER",
        "GMAIL_DRAFT_USER",
        default=EMAIL_ADDRESS,
    )
    DRAFT_EMAIL_APP_PASSWORD = _get_config_value(
        "DRAFT_GMAIL_PASSWORD",
        "GMAIL_DRAFT_PASSWORD",
        default=EMAIL_APP_PASSWORD,
    )


def _get_server_openai_api_key() -> str:
    return (_get_config_value("OPENAI_API_KEY", default="") or "").strip()


def _toml_escape_double_quoted(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace('"', '\\"')


def _toml_string_array(values: list[str]) -> str:
    out = [f'"{_toml_escape_double_quoted(v)}"' for v in values if str(v).strip()]
    return "[" + ", ".join(out) + "]"


def _read_optional_draft_secret_lines() -> str:
    """Preserve optional draft-account lines when rewriting hotel_credentials.toml."""
    if not SECRETS_TOML_PATH.exists():
        return ""
    keep: list[str] = []
    try:
        for line in SECRETS_TOML_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
            s = line.strip()
            if s.startswith("#") or not s:
                continue
            if s.upper().startswith("DRAFT_") or s.upper().startswith("GMAIL_DRAFT"):
                keep.append(line.rstrip())
    except Exception:
        return ""
    if not keep:
        return ""
    return "\n".join(["", "## Optional: save drafts to a different Gmail account.", *keep, ""])


def _write_secrets_toml(
    imap_server: str,
    gmail_user: str,
    gmail_password: str,
    openai_api_key: str,
    primary_language: str,
    spoken_languages: list[str],
) -> None:
    SECRETS_TOML_PATH.parent.mkdir(parents=True, exist_ok=True)
    tail = _read_optional_draft_secret_lines()
    imap_host = _normalize_imap_server_address(imap_server)
    spoken_clean = [str(x).strip() for x in (spoken_languages or []) if str(x).strip()]
    body = (
        "## Managed by AI Complaint Handler — do not commit real secrets to public repositories.\n"
        f'OPENAI_API_KEY = "{_toml_escape_double_quoted(openai_api_key)}"\n'
        f'IMAP_SERVER = "{_toml_escape_double_quoted(imap_host)}"\n'
        f'GMAIL_USER = "{_toml_escape_double_quoted(gmail_user)}"\n'
        f'GMAIL_PASSWORD = "{_toml_escape_double_quoted(gmail_password)}"\n'
        f'PRIMARY_LANGUAGE = "{_toml_escape_double_quoted(primary_language)}"\n'
        f"SPOKEN_LANGUAGES = {_toml_string_array(spoken_clean)}\n"
        f"{tail}"
    )
    SECRETS_TOML_PATH.write_text(body, encoding="utf-8")


def _persist_language_settings_to_toml() -> None:
    """Persist current language selections without requiring Save & Verify."""
    primary_clean = str(st.session_state.get("primary_language") or "").strip()
    if primary_clean not in LANGUAGE_OPTIONS:
        return
    spoken_clean = _coerce_spoken_languages_list(st.session_state.get("spoken_languages"))
    spoken_clean = [lang for lang in spoken_clean if lang in LANGUAGE_OPTIONS]
    try:
        _write_secrets_toml(
            _get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT),
            _get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default=""),
            _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default=""),
            _get_config_value("OPENAI_API_KEY", default=""),
            primary_clean,
            spoken_clean,
        )
    except Exception:
        return


def _read_automation_status_flag() -> str:
    if not AUTOMATION_STATUS_PATH.exists():
        return "STOPPED"
    try:
        raw = (AUTOMATION_STATUS_PATH.read_text(encoding="utf-8", errors="ignore") or "").strip().upper()
    except Exception:
        return "STOPPED"
    return raw if raw in {"RUNNING", "STOPPED"} else "STOPPED"


def _write_automation_status_flag(status: str) -> None:
    val = (status or "").strip().upper()
    if val not in {"RUNNING", "STOPPED"}:
        return
    try:
        AUTOMATION_STATUS_PATH.write_text(val, encoding="utf-8")
    except Exception:
        return


def _verify_imap_login(imap_server: str, gmail_user: str, gmail_app_password: str) -> None:
    host = _normalize_imap_server_address(imap_server)
    imap = imaplib.IMAP4_SSL(host)
    try:
        imap.login(gmail_user.strip(), gmail_app_password.strip())
        st_sel, _ = imap.select("INBOX")
        if (st_sel or "").upper() != "OK":
            raise RuntimeError(f"IMAP INBOX select failed: status={st_sel!r}")
    finally:
        try:
            imap.logout()
        except Exception:
            pass


def _verify_openai_api_key(api_key: str) -> None:
    key = (api_key or "").strip()
    if not key:
        raise ValueError("OpenAI API key is empty.")
    client = OpenAI(api_key=key)
    # Minimal API call to validate the key (cheap model, tiny completion).
    client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "Reply with exactly: ok"}],
        max_tokens=3,
        temperature=0,
    )


# --- UI strings (English only, B2B SaaS) ---
UI: dict[str, dict[str, str]] = {
    "English": {
        "kb_header": "Knowledge base",
        "primary_lang": "Primary language (for internal translations)",
        "spoken_langs": "Spoken languages (comma-separated)",
        "openai_missing": "Security warning: server configuration is incomplete. Missing `OPENAI_API_KEY` in `hotel_credentials.toml`, Streamlit secrets, or environment variables.",
        "active_db": "Active database:",
        "active_kb_loaded": "Active knowledge base loaded",
        "upload_new": "Upload Master Database (Excel)",
        "upload_help": "Uploading overwrites the saved knowledge base on disk.",
        "delete_db_btn": "Delete active database",
        "delete_db_ok": "Saved knowledge base removed. You can upload a new file.",
        "title": "Flow AI MailBot",
        "queue_metric": "Unread messages in queue",
        "queue_metric_value": "messages",
        "refresh_queue": "Refresh list",
        "search_label": "Search",
        "search_ph": "Search messages (empty: latest 50)…",
        "no_emails": "No messages found.",
        "unknown_sender": "(unknown sender)",
        "no_subject": "(no subject)",
        "load_email_failed": "Could not load the selected email:",
        "incoming_complaint": "Incoming complaint",
        "incoming_ph": "Paste the guest complaint here…",
        "generate_btn": "Generate AI reply",
        "ai_editor": "AI reply editor",
        "generated_editable": "Generated reply (editable before send)",
        "generated_ph": "The generated reply appears here. Edit before sending…",
        "send_btn": "Send reply",
        "send_success": "Reply sent and marked as Answered in Gmail.",
        "send_failed": "Could not send the reply:",
        "auto_load_failed": "Could not auto-load the saved knowledge base:",
        "save_kb_failed": "Could not save the knowledge base to disk:",
        "answered": "Answered",
        "read": "Read",
        "new": "New",
        "settings_panel": "System Settings & Credentials",
        "settings_imap": "IMAP server address",
        "settings_imap_help": "Hostname only (e.g. `imap.gmail.com`, `outlook.office365.com`). Gmail default: `imap.gmail.com`.",
        "settings_imap_placeholder": "e.g., imap.gmail.com",
        "settings_gmail": "Email Address",
        "settings_app_pw": "App Password",
        "settings_openai": "OpenAI API Key",
        "settings_save": "Save & Verify",
        "settings_success": "Credentials verified and saved to `hotel_credentials.toml`.",
        "settings_fail": "Verification or save failed.",
        "automata_status_running": "Automation: :green[**RUNNING**]",
        "automata_status_stopped": "Automation: :red[**STOPPED**]",
        "automata_start_btn": "Start automation",
        "automata_stop_btn": "Stop automation",
        "automata_wait_stop": "Automation is still stopping. Please wait.",
        "automata_need_verify": "Complete **Save & Verify** in System Settings (or fix invalid saved credentials) before starting automation.",
        "boot_verify_fail": "Saved credentials failed verification:",
        "original_message_section": "Original incoming email",
        "original_message_label": "Original message content",
        "suggested_reply_header": "Suggested Reply",
        "suggested_reply_label": "Suggested reply draft",
        "category_label": "Category",
        "no_original_message": "No original message available yet.",
    },
}


def _ui_lang() -> str:
    return "English"


def t(key: str) -> str:
    lang = _ui_lang()
    return UI.get(lang, {}).get(key, key)


def decode_mime_words(value: str) -> str:
    """
    Decode RFC 2047 'encoded-word' headers like '=?UTF-8?B?...' into readable text.
    """
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


PRIMARY_LANGUAGE_DEFAULT = "English"
LANGUAGE_OPTIONS = [
    "Afrikaans",
    "Albanian",
    "Amharic",
    "Arabic",
    "Armenian",
    "Azerbaijani",
    "Basque",
    "Bengali",
    "Bosnian",
    "Bulgarian",
    "Burmese",
    "Catalan",
    "Chinese (Cantonese)",
    "Chinese (Mandarin)",
    "Croatian",
    "Czech",
    "Danish",
    "Dutch",
    "English (Australia)",
    "English (UK)",
    "English (US)",
    "Estonian",
    "Filipino",
    "Finnish",
    "French (Canada)",
    "French (France)",
    "Galician",
    "Georgian",
    "German",
    "Greek",
    "Gujarati",
    "Hebrew",
    "Hindi",
    "Hungarian",
    "Icelandic",
    "Indonesian",
    "Irish",
    "Italian",
    "Japanese",
    "Kannada",
    "Kazakh",
    "Khmer",
    "Korean",
    "Latvian",
    "Lithuanian",
    "Macedonian",
    "Malay",
    "Malayalam",
    "Maltese",
    "Marathi",
    "Mongolian",
    "Nepali",
    "Norwegian",
    "Persian (Farsi)",
    "Polish",
    "Portuguese (Brazil)",
    "Portuguese (Portugal)",
    "Punjabi",
    "Romanian",
    "Russian",
    "Serbian",
    "Sinhala",
    "Slovak",
    "Slovenian",
    "Spanish (Latin America)",
    "Spanish (Spain)",
    "Swahili",
    "Swedish",
    "Tamil",
    "Telugu",
    "Thai",
    "Turkish",
    "Ukrainian",
    "Urdu",
    "Uzbek",
    "Vietnamese",
    "Welsh",
    "Zulu",
]
SPOKEN_LANGUAGES_DEFAULT_LIST = ["English", "Hungarian"]
SPOKEN_LANGUAGES_DEFAULT = "English, Hungarian"
POLL_INTERVAL_SECONDS = 30
IMAP_TIMEOUT_SECONDS = 10


def _normalize_spoken_languages_for_prompt(spoken_languages: str | list[str] | tuple[str, ...] | None) -> str:
    if isinstance(spoken_languages, (list, tuple)):
        cleaned = [str(x).strip() for x in spoken_languages if str(x).strip()]
        return ", ".join(cleaned)
    return str(spoken_languages or "").strip()


def _coerce_spoken_languages_list(value: object) -> list[str]:
    if isinstance(value, list):
        out = [str(x).strip() for x in value if str(x).strip()]
        return out
    if isinstance(value, tuple):
        out = [str(x).strip() for x in value if str(x).strip()]
        return out
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",") if p.strip()]
        return parts
    return []


def _assert_not_stopped(stop_event: threading.Event | None) -> None:
    if stop_event is not None and stop_event.is_set():
        raise InterruptedError("Automation stop requested.")


def _sleep_with_stop(seconds: float, stop_event: threading.Event | None) -> None:
    if seconds <= 0:
        return
    deadline = time.time() + float(seconds)
    while time.time() < deadline:
        _assert_not_stopped(stop_event)
        remaining = deadline - time.time()
        time.sleep(min(0.1, max(0.0, remaining)))


def get_unread_count() -> int:
    """
    Fast inbox queue metric: return count of unread (UNSEEN) messages in INBOX.
    Poka‑Yoke: never crash the UI; on network/auth errors return 0.
    """
    try:
        imap = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS))
        try:
            imap.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
            imap.select("INBOX")
            status, response = imap.uid("search", None, "UNSEEN")
            if status != "OK":
                return 0
            ids = (response[0] or b"").split()
            return int(len(ids))
        finally:
            try:
                imap.logout()
            except Exception:
                pass
    except Exception:
        return 0


def get_email_headers(search_term: str = "") -> list[dict]:
    """
    Interactive triage queue: return message UID + sender + subject + flags.
    Poka‑Yoke: on network/auth errors return [] (UI should show an error).

    - If search_term is empty: search ALL, but return only the last 50 for speed.
    - If search_term is present: use IMAP SEARCH TEXT across all mail, return up to 50 newest matches.
    """
    try:
        imap = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS))
        try:
            imap.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
            imap.select("INBOX")

            term = (search_term or "").strip()
            if term:
                # Gmail supports TEXT search over the whole mailbox.
                status, response = imap.uid("search", None, "TEXT", term)
            else:
                status, response = imap.uid("search", None, "ALL")
            if status != "OK":
                return []

            uids = [x for x in (response[0] or b"").split() if x]
            if not uids:
                return []

            try:
                uids = sorted(uids, key=lambda b: int(b))
            except Exception:
                pass

            # Fast UI: always cap at 50 newest results.
            uids = uids[-50:]
            uids = list(reversed(uids))  # newest first
            items: list[dict] = []
            for uid in uids:
                fstatus, fdata = imap.uid(
                    "fetch",
                    uid,
                    "(FLAGS BODY.PEEK[HEADER.FIELDS (FROM SUBJECT)])",
                )
                if fstatus != "OK" or not fdata or not fdata[0]:
                    continue

                raw = fdata[0][1] or b""
                msg = email.message_from_bytes(raw)

                flags_raw = ""
                try:
                    flags_raw = (fdata[0][0] or b"").decode(errors="ignore")
                except Exception:
                    flags_raw = ""

                answered = "\\Answered" in flags_raw
                seen = "\\Seen" in flags_raw
                if answered:
                    status_code = "answered"
                elif seen:
                    status_code = "read"
                else:
                    status_code = "new"

                sender = decode_mime_words(msg.get("From") or "")
                subject = decode_mime_words(msg.get("Subject", ""))

                items.append(
                    {
                        "uid": uid.decode(errors="ignore") if isinstance(uid, bytes) else str(uid),
                        "from": sender,
                        "subject": subject,
                        "status": status_code,
                    }
                )

            return items
        finally:
            try:
                imap.logout()
            except Exception:
                pass
    except Exception:
        return []
st.set_page_config(
    page_title="Flow AI MailBot",
    page_icon="🤖",
    layout="wide"
)


def _ensure_boot_credential_state() -> None:
    """
    On first run of a Streamlit session, verify credentials loaded from env/secrets.
    Automation may start only after verification succeeds (or after Save & Verify).
    """
    if st.session_state.get("_boot_credential_checked"):
        return
    st.session_state["_boot_credential_checked"] = True
    st.session_state.pop("boot_credential_error", None)

    em = (EMAIL_ADDRESS or "").strip()
    pw = (EMAIL_APP_PASSWORD or "").strip()
    ok = _get_server_openai_api_key()
    if not (em and pw and ok):
        st.session_state["system_credentials_verified"] = False
        return

    try:
        _verify_imap_login(IMAP_SERVER_ADDRESS, em, pw)
        _verify_openai_api_key(ok)
        st.session_state["system_credentials_verified"] = True
    except Exception as e:
        st.session_state["system_credentials_verified"] = False
        st.session_state["boot_credential_error"] = str(e)


def _auto_load_saved_knowledge_base() -> None:
    """
    Persistence: if a saved KB exists on disk, load it into session_state
    so the user doesn't need to re-upload after refresh.
    """
    if st.session_state.get("kb_df") is not None:
        return

    if not SAVED_KB_XLSX_PATH.exists():
        return

    p = SAVED_KB_XLSX_PATH
    try:
        file_bytes = p.read_bytes()
        kb_df = _normalize_columns(_read_knowledge_base_from_bytes(p.name, file_bytes))
        # Prefer the original uploaded filename if present.
        orig_name = None
        try:
            if SAVED_KB_ORIG_NAME_PATH.exists():
                orig_name = (SAVED_KB_ORIG_NAME_PATH.read_text(encoding="utf-8") or "").strip()
        except Exception:
            orig_name = None
        st.session_state["kb_file_name"] = orig_name or p.name
        st.session_state["kb_file_bytes"] = file_bytes
        st.session_state["kb_df"] = kb_df
        st.session_state["kb_loaded"] = True
        st.session_state["kb_shape"] = (int(len(kb_df)), int(len(kb_df.columns)))
    except Exception as e:
        st.session_state["kb_loaded"] = False
        st.session_state["kb_shape"] = (0, 0)
        st.error(f"{t('auto_load_failed')} {e}")


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

    raise ValueError("Unsupported file format. Please upload a CSV or Excel file.")


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


def _kb_rows_for_prompt(kb: pd.DataFrame, max_rows: int = 200) -> list[dict]:
    kb = _normalize_columns(kb)
    if "Category" not in kb.columns or "Policy" not in kb.columns:
        raise ValueError("Your knowledge base must contain 'Category' and 'Policy' columns (CSV/Excel headers).")

    rows = []
    for _, r in kb[["Category", "Policy"]].dropna(how="all").head(max_rows).iterrows():
        cat = "" if pd.isna(r["Category"]) else str(r["Category"]).strip()
        pol = "" if pd.isna(r["Policy"]) else str(r["Policy"]).strip()
        if cat or pol:
            rows.append({"category": cat, "policy": pol})
    if not rows:
        raise ValueError("Your knowledge base is empty, or the relevant columns have no usable values.")
    return rows


def _get_openai_client() -> OpenAI:
    key = _get_server_openai_api_key()
    if not key or key in {"", "ide-masold-be-a-kulcsot"}:
        raise ValueError(
            "Missing OpenAI API key. Set OPENAI_API_KEY in hotel_credentials.toml, Streamlit secrets, or environment variables."
        )
    return OpenAI(api_key=key)


def _parse_json_object_maybe_fenced(text: str) -> dict:
    s = (text or "").strip()
    if not s:
        raise ValueError("Empty response from the model.")

    # Remove Markdown code fences if present (```json ... ```).
    if s.startswith("```"):
        lines = s.splitlines()
        if lines and lines[0].lstrip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()

    # Sometimes the model still wraps JSON in stray backticks.
    s = s.strip().strip("`").strip()
    return json.loads(s)


def _auto_clean_outgoing_body(text: str) -> str:
    """
    Poka‑Yoke: strip internal-only blocks from the outgoing email.

    Removes:
    - An optional leading "[ACTION REQUIRED: ...]" line.
    - An internal interpreter block starting with "[--- BELSŐ INFÓ ---]" (or similar)
      up to the first blank-line break that precedes the customer-facing reply.
    """
    if not text:
        return ""

    s = str(text).replace("\r\n", "\n").replace("\r", "\n")

    # Remove the "ACTION REQUIRED" line if present (internal-only marker).
    s = re.sub(r"(?m)^\[ACTION REQUIRED:[^\]]*\]\s*\n?", "", s).lstrip()

    lines = s.split("\n")
    out: list[str] = []
    skipping = False
    saw_internal_header = False

    def _is_internal_header(line: str) -> bool:
        l = (line or "").strip()
        if not l.startswith("["):
            return False
        # Common separators coming from the prompt.
        return ("BELSŐ" in l.upper() and "INF" in l.upper()) or ("BELSO" in l.upper() and "INFO" in l.upper())

    i = 0
    while i < len(lines):
        line = lines[i]

        if not skipping and _is_internal_header(line):
            skipping = True
            saw_internal_header = True
            i += 1
            continue

        if skipping:
            # End skipping once we hit a blank line AND the next non-empty line looks like normal text.
            if line.strip() == "":
                # Look ahead to next non-empty.
                j = i + 1
                while j < len(lines) and lines[j].strip() == "":
                    j += 1
                if j >= len(lines) or not lines[j].strip().startswith("["):
                    skipping = False
                    # Keep a single blank line as separator before the customer reply.
                    out.append("")
                    i = j
                    continue
            i += 1
            continue

        out.append(line)
        i += 1

    cleaned = "\n".join(out)

    # If the model produced a different bracketed "internal" block delimiter, also remove
    # the most common pattern of bracketed internal section at the top.
    if saw_internal_header:
        return cleaned.strip() + "\n"
    return cleaned.strip() + ("\n" if cleaned.strip() else "")


def _append_original_message_block(draft_body: str, original_body: str) -> str:
    """
    Append the original inbound message below the generated draft text.
    Supports plain-text and simple HTML bodies.
    """
    draft = (draft_body or "").strip()
    original = (original_body or "").strip()
    if not original:
        return draft + ("\n" if draft else "")

    # Very lightweight HTML detection.
    is_html = bool(re.search(r"<\s*(html|body|div|p|br|span|table|tr|td)\b", draft, flags=re.IGNORECASE))

    if is_html:
        # Keep HTML output readable and safe for plain text snippets.
        safe_original = (
            original.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        if draft:
            return (
                f"{draft}\n"
                "<hr>"
                "<p><strong>Original Message</strong></p>"
                f"<pre>{safe_original}</pre>\n"
            )
        return f"<p><strong>Original Message</strong></p><pre>{safe_original}</pre>\n"

    # Default plain text formatting.
    if draft:
        return f"{draft}\n\n--- Original Message ---\n\n{original}\n"
    return f"--- Original Message ---\n\n{original}\n"


def send_reply_email(
    to_address: str,
    subject: str,
    body: str,
    original_message_id: str | None,
    original_uid: str | None = None,
) -> None:
    """
    Send an SMTP email reply that stays in the original Gmail thread.
    """
    to_address = (to_address or "").strip()
    if not to_address:
        raise ValueError("Missing recipient email address.")

    subject = (subject or "").strip() or "Re:"
    body = (body or "").strip()
    if not body:
        raise ValueError("Email body is empty.")

    msg = MIMEMultipart()
    msg["From"] = GMAIL_USER
    msg["To"] = to_address
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()

    orig = (original_message_id or "").strip()
    if orig:
        # Gmail threads by these headers.
        msg["In-Reply-To"] = orig
        msg["References"] = orig

    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, [to_address], msg.as_string())
    except Exception as e:
        raise RuntimeError(f"SMTP send error: {e}")

    # After a successful send, mark the original message as Answered via IMAP.
    uid_clean = (original_uid or "").strip()
    if uid_clean:
        imap = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS))
        try:
            imap.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
            imap.select("INBOX")
            try:
                imap.uid("store", uid_clean.encode("utf-8"), "+FLAGS", "\\Answered")
            except Exception:
                # Best-effort only: do not fail the already-sent SMTP operation.
                pass
        finally:
            try:
                imap.logout()
            except Exception:
                pass


def fetch_email_by_uid(
    uid: str,
    mark_as_read: bool = False,
    source_mailbox: str = "INBOX",
    stop_event: threading.Event | None = None,
) -> dict:
    """
    Fetch a single email by UID (used by the interactive triage queue).
    """
    uid_clean = (uid or "").strip()
    if not uid_clean:
        raise ValueError("Missing UID.")

    _assert_not_stopped(stop_event)
    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS), timeout=IMAP_TIMEOUT_SECONDS)
    try:
        _assert_not_stopped(stop_event)
        mail.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
        mailbox = (source_mailbox or "INBOX").strip() or "INBOX"
        _assert_not_stopped(stop_event)
        mail.select(mailbox)

        _assert_not_stopped(stop_event)
        fstatus, fdata = mail.uid("fetch", uid_clean.encode("utf-8"), "(RFC822)")
        if fstatus != "OK" or not fdata or not fdata[0]:
            raise RuntimeError("Unable to fetch message by UID.")

        raw = fdata[0][1] or b""
        msg = email.message_from_bytes(raw)

        subject = decode_mime_words(msg.get("Subject", ""))
        sender = decode_mime_words(msg.get("From", ""))
        date = (msg.get("Date") or "").strip()
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
            if not body_text:
                for part in msg.walk():
                    ctype = part.get_content_type()
                    disp = (part.get("Content-Disposition") or "").lower()
                    if "attachment" in disp:
                        continue
                    if ctype == "text/html":
                        payload = part.get_payload(decode=True) or b""
                        charset = part.get_content_charset() or "utf-8"
                        body_text = payload.decode(charset, errors="replace").strip()
                        break
        else:
            payload = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            body_text = payload.decode(charset, errors="replace").strip()

        if mark_as_read:
            try:
                _assert_not_stopped(stop_event)
                mail.uid("store", uid_clean.encode("utf-8"), "+FLAGS", "\\Seen")
            except Exception:
                pass

        return {
            "uid": uid_clean,
            "source_mailbox": mailbox,
            "from": sender,
            "subject": subject,
            "date": date,
            "message_id": message_id,
            "body": body_text,
        }
    except Exception as e:
        raise RuntimeError(f"IMAP fetch-by-UID error: {e}")
    finally:
        try:
            mail.logout()
        except Exception:
            pass


def _flag_email_in_inbox(uid: str) -> None:
    uid = (uid or "").strip()
    if not uid:
        return

    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS))
    try:
        mail.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
        mail.select("INBOX")
        mail.uid("store", uid.encode("utf-8"), "+FLAGS", "\\Flagged")
    except Exception:
        # Non-fatal: do not block operator flow if flagging fails.
        return
    finally:
        try:
            mail.logout()
        except Exception:
            pass

def _select_best_policy(complaint: str, kb_rows: list[dict]) -> dict:
    client = _get_openai_client()

    system = (
        "You are a customer-support triage assistant. Your job is to select the single most relevant policy entry "
        "from the provided knowledge base for the incoming complaint.\n\n"
        "Use deep semantic matching, language-agnostic reasoning, and intent-level understanding.\n"
        "The incoming complaint can be in any language; ALWAYS map meaning to policy categories regardless of language.\n"
        "If multiple categories are partially present, you MUST select the single most relevant category that best resolves the main customer intent.\n"
        "Do not return UNKNOWN just because there are mixed signals or overlapping details.\n\n"
        "Return match=false ONLY when the complaint is truly off-domain and has no meaningful policy coverage at all "
        "(for example: dog purchase, weather, unrelated personal topics).\n"
        "If at least one policy entry is reasonably applicable, return match=true and pick the best one.\n\n"
        "Hard requirement: respond with a valid JSON object only (no extra text, no Markdown, no code fences)."
    )
    user = (
        "Incoming complaint:\n"
        f"{complaint}\n\n"
        "Knowledge base (JSON array: category, policy):\n"
        f"{json.dumps(kb_rows, ensure_ascii=False)}\n\n"
        "Return ONLY a JSON object using this schema:\n"
        "{\n"
        '  "match": true|false,\n'
        '  "confidence": 0.0-1.0,\n'
        '  "category": "…",\n'
        '  "policy": "…",\n'
        '  "reason": "1-2 sentence justification"\n'
        "}\n"
        "When match=true: fill category and policy by copying exactly one entry from the knowledge base.\n"
        "When match=false: set confidence <= 0.4 and leave category/policy empty strings.\n"
        "When match=true: prefer confidence >= 0.7 unless the evidence is genuinely weak."
    )

    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )
    content = (resp.choices[0].message.content or "").strip()
    try:
        return _parse_json_object_maybe_fenced(content)
    except Exception as e:
        return {
            "match": False,
            "confidence": 0.0,
            "category": "",
            "policy": "",
            "reason": f"Failed to parse the model's JSON response: {e}",
        }


def _generate_reply_letter(complaint: str, selection: dict, primary_language: str, spoken_languages: str) -> str:
    complaint_raw = (complaint or "")
    complaint = complaint_raw.strip()
    if not complaint:
        return "Please paste the incoming complaint, then click “Generate Response”."
    complaint_for_output = re.sub(r"\r\n?", "\n", complaint).strip()
    complaint_for_output = re.sub(r"\n{2,}", "\n\n", complaint_for_output)

    primary_language = (primary_language or "").strip() or PRIMARY_LANGUAGE_DEFAULT
    spoken_languages_norm = _normalize_spoken_languages_for_prompt(spoken_languages)

    match = bool(selection.get("match"))
    confidence = float(selection.get("confidence") or 0.0)
    category = (selection.get("category") or "").strip()
    policy = (selection.get("policy") or "").strip()
    reason = (selection.get("reason") or "").strip()

    client = _get_openai_client()
    is_unknown_case = (not match) or confidence <= 0.4 or not policy
    case_name = "UNKNOWN" if is_unknown_case else "KNOWN"
    system = (
        "You are a strict email-draft formatter.\n"
        "First, detect the language of the incoming original customer email.\n"
        "CRITICAL RESPONSE-LANGUAGE RULE: line1 MUST be written in the detected incoming email language.\n"
        "Never answer the customer in Primary language unless the incoming email is actually in Primary language.\n"
        "Knowledge-base language is irrelevant for output language; it is only guidance for policy content.\n"
        f"CRITICAL RULE: Evaluate the detected language of the incoming email. If it is NOT exactly '{primary_language}' AND NOT in the list of '{spoken_languages_norm or '(none)'}', you MUST append a new line with '--- INTERNAL TRANSLATION ---' at the very end of your response. Under the '--- INTERNAL TRANSLATION ---' line, you MUST strictly translate the original customer email text into '{primary_language}'. Do not just copy the original foreign text. Then, also translate your generated response into '{primary_language}'. Failure to include this internal translation block for foreign languages will result in a system failure.\n"
        "Return ONLY valid JSON (no markdown, no extra text) with EXACTLY these keys:\n"
        "{\n"
        '  "line1": "...",\n'
        '  "internal_translation_block": "..."\n'
        "}\n\n"
        "Rules:\n"
        f"- Primary language = {primary_language}.\n"
        f"- Spoken languages list (treated as understood by staff) = {spoken_languages_norm or '(none)'}.\n"
        "- line1 must be a single line (no newline characters).\n"
        "- NEVER include markdown code fences.\n"
        "- line1 MUST always be in the incoming email language; this rule has highest priority.\n"
        "- If Case type is UNKNOWN, line1 MUST be the Primary-language equivalent of this exact message:\n"
        '  "[ACTION REQUIRED] I could not process this email automatically."\n'
        "- Determine whether internal translation is needed:\n"
        "  * If incoming language == Primary language OR incoming language is included in Spoken languages: internal_translation_block MUST be empty.\n"
        "  * Otherwise (third/foreign language): internal_translation_block is MANDATORY and MUST be in Primary language.\n"
        "- For required internal_translation_block, include in this order:\n"
        "  1) A separator line translated into Primary language, equivalent to: --- INTERNAL TRANSLATION ---\n"
        "  2) Translation of the incoming customer email into Primary language.\n"
        "  3) Translation of line1 into Primary language.\n"
        "- Special fallback rule for UNKNOWN + third/foreign language:\n"
        "  internal_translation_block must include at least the translated incoming customer email in Primary language.\n"
        "- Never include signatures, greetings metadata, or extra sections outside the required content."
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
        "Selection rationale (internal):\n"
        f"{reason or '(none)'}\n\n"
        "Generate JSON exactly as requested."
    )
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
    )
    try:
        payload = _parse_json_object_maybe_fenced(resp.choices[0].message.content or "")
    except Exception:
        payload = {}
    line1 = re.sub(r"[\r\n]+", " ", str(payload.get("line1") or "")).strip()
    internal_translation_block = str(payload.get("internal_translation_block") or "")
    internal_translation_block = internal_translation_block.replace("\r\n", "\n").replace("\r", "\n").strip()

    if not line1:
        if is_unknown_case:
            line1 = "[ACTION REQUIRED] I could not process this email automatically."
        else:
            line1 = "Thank you for your message. We are reviewing your case based on our policy."

    if internal_translation_block:
        return (
            f"{line1}\n"
            f"{internal_translation_block}\n"
            "--- Original Message ---\n"
            f"{complaint_for_output}"
        )
    return (
        f"{line1}\n"
        "--- Original Message ---\n"
        f"{complaint_for_output}"
    )


def _run_generate_response(complaint_text: str, kb_df: pd.DataFrame | None) -> None:
    if kb_df is None or kb_df.empty:
        raise ValueError("Please upload a knowledge base (CSV/Excel) in the sidebar.")

    complaint_clean = (complaint_text or "").strip()
    if not complaint_clean:
        raise ValueError("Please enter the incoming complaint text.")

    with st.spinner("Finding the best matching policy in your knowledge base…"):
        kb_rows = _kb_rows_for_prompt(kb_df)
        selection = _select_best_policy(complaint_clean, kb_rows)
        st.session_state["last_selection"] = selection
        try:
            pol = (selection.get("policy") or "").strip()
            conf = float(selection.get("confidence") or 0.0)
            m = bool(selection.get("match"))
            st.session_state["is_unique_case"] = (not m) or conf <= 0.4 or (not pol)
        except Exception:
            st.session_state["is_unique_case"] = True

    with st.spinner("Generating reply draft…"):
        draft = _generate_reply_letter(
            complaint_clean,
            selection,
            primary_language=_get_persisted_primary_language(),
            spoken_languages=_normalize_spoken_languages_for_prompt(_get_persisted_spoken_languages()),
        )
        # Source of truth for the editable draft in the UI.
        st.session_state["draft_response"] = draft
        # Ensure the text area updates immediately after generation.
        st.session_state["editable_draft"] = draft

        # Backwards-compatible keys (used elsewhere in this file).
        st.session_state["draft_reply"] = draft
        st.session_state["editable_reply"] = draft
        if st.session_state.get("is_unique_case"):
            latest_email = st.session_state.get("latest_email") or {}
            _flag_email_in_inbox(latest_email.get("uid", ""))


def _load_kb_df_from_disk() -> pd.DataFrame | None:
    if not SAVED_KB_XLSX_PATH.exists():
        return None
    try:
        file_bytes = SAVED_KB_XLSX_PATH.read_bytes()
        return _normalize_columns(_read_knowledge_base_from_bytes(SAVED_KB_XLSX_PATH.name, file_bytes))
    except Exception as e:
        print(f"[KB] Failed to load KB from disk: {e}")
        return None


def _build_reply_subject(original_subject: str) -> str:
    s = (original_subject or "").strip()
    if s.lower().startswith("re:"):
        return s
    if s:
        return f"Re: {s}"
    return "Re:"


def _generate_reply_draft_text(
    complaint_text: str,
    kb_df: pd.DataFrame,
    primary_language: str,
    spoken_languages: str,
) -> tuple[str, bool]:
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


def _discover_poll_mailboxes(mail: imaplib.IMAP4, stop_event: threading.Event | None = None) -> list[str]:
    """
    Return poll targets in priority order.
    Always include INBOX, plus common spam mailbox names when available.
    """
    candidates = ["INBOX", "[Gmail]/Spam", "Spam", "Junk"]
    discovered: list[str] = []
    for mailbox in candidates:
        try:
            _assert_not_stopped(stop_event)
            sel_status, sel_data = mail.select(mailbox)
            print(f"[IMAP] select {mailbox} for polling status={sel_status} data={sel_data}")
            if (sel_status or "").upper() == "OK":
                discovered.append(mailbox)
        except Exception as e:
            print(f"[IMAP] select {mailbox} for polling failed: {e}")
            continue

    out: list[str] = []
    for mb in discovered:
        if mb not in out:
            out.append(mb)
    return out or ["INBOX"]


def _search_unseen_uids(stop_event: threading.Event | None = None) -> list[tuple[str, str]]:
    """
    Return unread message refs as (mailbox, uid), newest-first per mailbox.
    """
    _assert_not_stopped(stop_event)
    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS), timeout=IMAP_TIMEOUT_SECONDS)
    try:
        _assert_not_stopped(stop_event)
        mail.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
        _assert_not_stopped(stop_event)
        mailboxes = _discover_poll_mailboxes(mail, stop_event=stop_event)
        refs: list[tuple[str, str]] = []
        seen_refs: set[tuple[str, str]] = set()

        for mailbox in mailboxes:
            _assert_not_stopped(stop_event)
            sel_status, sel_data = mail.select(mailbox)
            print(f"[IMAP] select {mailbox} for UNSEEN search status={sel_status} data={sel_data}")
            if (sel_status or "").upper() != "OK":
                continue

            _assert_not_stopped(stop_event)
            status, response = mail.search(None, "UNREAD")
            if (status or "").upper() != "OK":
                print(f"[IMAP] SEARCH UNREAD non-OK status={status} mailbox={mailbox} response={response}")
                continue

            raw = (response[0] or b"")
            seq_ids_b = [x for x in raw.split() if x]
            if not seq_ids_b:
                continue

            # Newest-first.
            try:
                seq_ids_b = sorted(seq_ids_b, key=lambda b: int(b), reverse=True)
            except Exception:
                seq_ids_b = list(reversed(seq_ids_b))

            for seq_id in seq_ids_b:
                _assert_not_stopped(stop_event)
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


def _safe_decode_imap_list_line(raw: bytes) -> str:
    """
    Best-effort decoding for IMAP LIST output lines.
    Gmail labels (e.g. Hungarian "Piszkozatok") can show up in IMAP modified UTF-7.
    """
    if not raw:
        return ""
    try:
        return raw.decode("utf-8", errors="replace")
    except Exception:
        try:
            # Latin-1 keeps bytes 1:1, useful as a last-resort fallback.
            return raw.decode("latin-1", errors="replace")
        except Exception:
            return str(raw)


def _extract_mailbox_name_from_list_line(line: str) -> tuple[list[str], str] | None:
    """
    Parse one IMAP LIST response line into (flags, mailbox_name).
    Example:
      (\\HasNoChildren \\Drafts) "/" "[Gmail]/Drafts"
    """
    s = (line or "").strip()
    if not s:
        return None

    # Typical format: (<flags>) "<delim>" <mailbox>
    m = re.match(r'^\((?P<flags>[^\)]*)\)\s+"(?P<delim>[^"]*)"\s+(?P<name>.+)$', s)
    if not m:
        # Sometimes delimiter isn't quoted.
        m = re.match(r"^\((?P<flags>[^\)]*)\)\s+(?P<delim>\S+)\s+(?P<name>.+)$", s)
    if not m:
        return None

    flags_raw = (m.group("flags") or "").strip()
    flags = [f for f in flags_raw.split() if f]
    name = (m.group("name") or "").strip()
    # Strip surrounding quotes if present.
    if len(name) >= 2 and name[0] == '"' and name[-1] == '"':
        name = name[1:-1]
    return flags, name


def _discover_gmail_drafts_mailbox(mail: imaplib.IMAP4) -> str:
    """
    Find the correct Drafts mailbox name for the current account.
    Preference order:
    - Mailbox with \\Drafts flag from LIST
    - Known Gmail names: [Gmail]/Piszkozatok, Piszkozatok, [Gmail]/Drafts, Drafts
    """
    fallback_names = ["[Gmail]/Piszkozatok", "Piszkozatok", "[Gmail]/Drafts", "Drafts"]

    try:
        status, boxes = mail.list()
    except Exception as e:
        print(f"[IMAP] mail.list() failed: {e} -> fallback names will be tried")
        return fallback_names[0]

    decoded_lines: list[str] = []
    if boxes:
        for raw in boxes:
            if isinstance(raw, bytes):
                decoded_lines.append(_safe_decode_imap_list_line(raw))
            else:
                decoded_lines.append(str(raw))

    print(f"[IMAP] mail.list() status={status} boxes={len(decoded_lines)}")
    if (status or "").upper() != "OK":
        print(f"[IMAP] mail.list() returned non-OK status={status!r}; will try fallbacks: {fallback_names}")
        return fallback_names[0]

    parsed: list[tuple[list[str], str]] = []
    for line in decoded_lines:
        parsed_item = _extract_mailbox_name_from_list_line(line)
        if parsed_item:
            parsed.append(parsed_item)

    # 1) Strong signal: \Drafts flag
    for flags, name in parsed:
        if any(f.upper() == "\\DRAFTS" for f in flags):
            if (name or "").strip().upper() == "INBOX":
                # Safety: never treat INBOX as Drafts.
                print(f"[IMAP] Ignoring Drafts-by-flag candidate because it is INBOX: name={name!r}")
                continue
            print(f"[IMAP] Drafts mailbox found by flag: name={name!r} flags={flags}")
            return name

    # 2) Fallback: try to match known mailbox names from the LIST output
    listed_names = [name for _, name in parsed]
    listed_lower = {n.lower(): n for n in listed_names if n}
    for candidate in fallback_names:
        found = listed_lower.get(candidate.lower())
        if found:
            if (found or "").strip().upper() == "INBOX":
                print(f"[IMAP] Ignoring Drafts-by-name candidate because it is INBOX: found={found!r}")
                continue
            print(f"[IMAP] Drafts mailbox found by name match: candidate={candidate!r} actual={found!r}")
            return found

    # 3) Last resort: return a reasonable default and let append attempts decide
    print(f"[IMAP] Drafts mailbox not identified from LIST; will try fallbacks: {fallback_names}")
    # Never return INBOX as a "drafts" mailbox.
    for fb in fallback_names:
        if (fb or "").strip().upper() != "INBOX":
            return fb
    return "[Gmail]/Drafts"


def _mark_original_seen_only(uid: str, also_flag: bool = False, source_mailbox: str = "INBOX") -> None:
    uid_clean = (uid or "").strip()
    if not uid_clean:
        return

    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS))
    try:
        mail.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
        mailbox = (source_mailbox or "INBOX").strip() or "INBOX"
        sel_status, sel_data = mail.select(mailbox)
        print(f"[IMAP] select {mailbox} status={sel_status} data={sel_data} uid={uid_clean}")

        st_status, st_data = mail.uid("store", uid_clean.encode("utf-8"), "+FLAGS", "\\Seen")
        print(f"[IMAP] uid STORE +FLAGS \\Seen status={st_status} data={st_data} uid={uid_clean}")

        if also_flag:
            fl_status, fl_data = mail.uid("store", uid_clean.encode("utf-8"), "+FLAGS", "\\Flagged")
            print(f"[IMAP] uid STORE +FLAGS \\Flagged status={fl_status} data={fl_data} uid={uid_clean}")
    finally:
        try:
            mail.logout()
        except Exception:
            pass


def _uid_store_seen_with_retry(
    mail: imaplib.IMAP4,
    mailbox: str,
    uid_clean: str,
    *,
    retries: int = 3,
    also_flag: bool = False,
    stop_event: threading.Event | None = None,
) -> bool:
    attempts = max(1, int(retries))
    for attempt in range(1, attempts + 1):
        _assert_not_stopped(stop_event)
        try:
            sel_status, sel_data = mail.select(mailbox)
            print(
                f"[IMAP] select {mailbox} for seen-sync attempt={attempt}/{attempts} "
                f"status={sel_status} data={sel_data} uid={uid_clean}"
            )
            if (sel_status or "").upper() != "OK":
                raise RuntimeError(f"select failed status={sel_status} data={sel_data}")

            st_status, st_data = mail.uid("store", uid_clean.encode("utf-8"), "+FLAGS", "\\Seen")
            print(
                f"[IMAP] uid STORE +FLAGS \\Seen attempt={attempt}/{attempts} "
                f"status={st_status} data={st_data} mailbox={mailbox} uid={uid_clean}"
            )
            if (st_status or "").upper() != "OK":
                raise RuntimeError(f"STORE +FLAGS \\Seen failed status={st_status} data={st_data}")

            if also_flag:
                try:
                    fl_status, fl_data = mail.uid("store", uid_clean.encode("utf-8"), "+FLAGS", "\\Flagged")
                    print(
                        f"[IMAP] uid STORE +FLAGS \\Flagged status={fl_status} data={fl_data} "
                        f"mailbox={mailbox} uid={uid_clean}"
                    )
                except Exception as e:
                    print(f"[IMAP] Optional \\Flagged failed mailbox={mailbox} uid={uid_clean}: {e}")
            return True
        except InterruptedError:
            raise
        except Exception as e:
            print(f"[IMAP] seen-sync retryable failure mailbox={mailbox} uid={uid_clean} attempt={attempt}: {e}")
            if attempt < attempts:
                _sleep_with_stop(0.6, stop_event)
    return False


def _mark_seen_in_poll_mailboxes(
    uid: str,
    also_flag: bool = False,
    stop_event: threading.Event | None = None,
    source_mailbox: str | None = None,
    retries: int = 3,
) -> bool:
    uid_clean = (uid or "").strip()
    if not uid_clean:
        return False

    _assert_not_stopped(stop_event)
    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS), timeout=IMAP_TIMEOUT_SECONDS)
    try:
        _assert_not_stopped(stop_event)
        mail.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
        source = (source_mailbox or "").strip()
        poll_mailboxes = _discover_poll_mailboxes(mail, stop_event=stop_event)
        targets: list[str] = []
        if source:
            targets.append(source)
        for mailbox in poll_mailboxes:
            if mailbox not in targets:
                targets.append(mailbox)

        source_marked = False
        any_marked = False
        for mailbox in targets:
            try:
                marked = _uid_store_seen_with_retry(
                    mail,
                    mailbox,
                    uid_clean,
                    retries=retries,
                    also_flag=also_flag,
                    stop_event=stop_event,
                )
                any_marked = any_marked or marked
                if source and mailbox == source:
                    source_marked = marked
            except InterruptedError:
                return False
            except Exception as e:
                print(f"[IMAP] seen-sync failed for mailbox={mailbox} uid={uid_clean}: {e}")
                continue
        if source:
            return source_marked
        return any_marked
    finally:
        try:
            mail.logout()
        except Exception:
            pass


def _save_reply_as_draft_and_mark_seen(
    uid: str,
    source_mailbox: str,
    to_address: str,
    reply_subject: str,
    clean_body: str,
    original_message_id: str | None,
    is_unique_case: bool,
    stop_event: threading.Event | None = None,
) -> None:
    uid_clean = (uid or "").strip()
    if not uid_clean:
        raise ValueError("Missing UID for original message.")

    to_address = (to_address or "").strip()
    if not to_address:
        raise ValueError("Missing draft recipient address.")

    if not (clean_body or "").strip():
        # Even if draft body is empty, still mark original as seen to prevent reprocessing.
        _mark_seen_in_poll_mailboxes(
            uid_clean,
            also_flag=is_unique_case,
            stop_event=stop_event,
            source_mailbox=source_mailbox,
            retries=4,
        )
        return

    msg = MIMEMultipart()
    msg["From"] = (DRAFT_EMAIL_ADDRESS or GMAIL_USER or EMAIL_ADDRESS).strip()
    msg["To"] = to_address
    msg["Subject"] = reply_subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()

    # CRITICAL: Do NOT include threading headers in the draft.
    # Gmail may associate the message with an existing thread and surface it in Inbox views.
    # We want an independent draft that exists ONLY in Drafts.

    msg.attach(MIMEText(clean_body, "plain", "utf-8"))
    msg_bytes = msg.as_string().encode("utf-8", errors="replace")

    _assert_not_stopped(stop_event)
    mail = imaplib.IMAP4_SSL(_normalize_imap_server_address(IMAP_SERVER_ADDRESS), timeout=IMAP_TIMEOUT_SECONDS)
    try:
        _assert_not_stopped(stop_event)
        mail.login(DRAFT_EMAIL_ADDRESS, DRAFT_EMAIL_APP_PASSWORD)

        _assert_not_stopped(stop_event)
        drafts_box = (_discover_gmail_drafts_mailbox(mail) or "").strip()
        if not drafts_box:
            raise RuntimeError("Drafts mailbox discovery returned empty name.")
        if drafts_box.upper() == "INBOX":
            raise RuntimeError("Safety violation: Drafts mailbox resolved to INBOX.")

        # STRICT: append ONLY to the identified Drafts mailbox (no fallbacks).
        print(f"[IMAP] APPEND draft STRICT -> mailbox={drafts_box!r}")
        _assert_not_stopped(stop_event)
        ap_status, ap_data = mail.append(drafts_box, "(\\Draft \\Seen)", None, msg_bytes)
        print(f"[IMAP] APPEND result status={ap_status} data={ap_data} mailbox={drafts_box!r}")
        if (ap_status or "").upper() != "OK":
            raise RuntimeError(f"APPEND returned non-OK status={ap_status} data={ap_data}")
    finally:
        try:
            mail.logout()
        except Exception:
            pass

    # Mark the original inbound email as read (on the original inbox account).
    # NOTE: marking the original as Seen is done by the caller immediately
    # after successful APPEND, inside the per-message processing loop.


def _automata_worker_loop(primary_language: str, spoken_languages: str, stop_event: threading.Event) -> None:
    primary_language = (primary_language or "").strip() or PRIMARY_LANGUAGE_DEFAULT
    spoken_languages = _normalize_spoken_languages_for_prompt(spoken_languages)
    processed_email_ids: set[tuple[str, str]] = set()
    print(f"[Automata] Worker language params: primary_language={primary_language!r} spoken_languages={spoken_languages!r}")
    print("[Automata] Worker started.")
    while True:
        if stop_event.is_set():
            print("[Automata] Stop requested, worker exiting.")
            break
        try:
            if not EMAIL_ADDRESS or not EMAIL_APP_PASSWORD:
                print("[Automata] Gmail credentials missing; sleeping.")
                _sleep_with_stop(POLL_INTERVAL_SECONDS, stop_event)
                continue

            server_openai_key = _get_server_openai_api_key()
            if not server_openai_key or server_openai_key in {"", "ide-masold-be-a-kulcsot"}:
                print("[Automata] OPENAI_API_KEY missing; sleeping.")
                _sleep_with_stop(POLL_INTERVAL_SECONDS, stop_event)
                continue

            kb_df = _load_kb_df_from_disk()
            if kb_df is None or kb_df.empty:
                print("[Automata] Knowledge base missing/empty; sleeping.")
                _sleep_with_stop(POLL_INTERVAL_SECONDS, stop_event)
                continue

            _assert_not_stopped(stop_event)
            uid_refs = _search_unseen_uids(stop_event=stop_event)
            if not uid_refs:
                _sleep_with_stop(POLL_INTERVAL_SECONDS, stop_event)
                continue

            found_mailboxes = sorted({mb for mb, _ in uid_refs})
            print(f"[Automata] UNSEEN found: {len(uid_refs)} message(s) across {found_mailboxes}.")
            for source_mailbox, uid in uid_refs:
                if stop_event.is_set():
                    print("[Automata] Stop requested during batch, worker exiting.")
                    return
                email_ref = (source_mailbox, uid)
                if email_ref in processed_email_ids:
                    continue

                try:
                    _assert_not_stopped(stop_event)
                    print(f"[Automata] Processing mailbox={source_mailbox} UID={uid} ...")
                    email_obj = fetch_email_by_uid(uid, mark_as_read=False, source_mailbox=source_mailbox, stop_event=stop_event)
                    complaint_text = (email_obj.get("body") or "").strip()
                    if not complaint_text:
                        source_seen_ok = _mark_seen_in_poll_mailboxes(
                            uid,
                            also_flag=False,
                            stop_event=stop_event,
                            source_mailbox=source_mailbox,
                            retries=4,
                        )
                        if not source_seen_ok:
                            print(
                                f"[Automata] Could not confirm \\Seen on source mailbox={source_mailbox} "
                                f"UID={uid}; stopping batch before next email."
                            )
                            break
                        continue

                    _assert_not_stopped(stop_event)
                    draft_text, is_unique_case = _generate_reply_draft_text(
                        complaint_text,
                        kb_df,
                        primary_language=primary_language,
                        spoken_languages=spoken_languages,
                    )
                    draft_mode = "UNKNOWN" if is_unique_case else "KNOWN"
                    print(f"[Automata] Draft mode={draft_mode} mailbox={source_mailbox} UID={uid}")
                    clean_body = _auto_clean_outgoing_body(draft_text)
                    to_address = parseaddr(email_obj.get("from") or "")[1].strip()
                    reply_subject = _build_reply_subject(email_obj.get("subject") or "")
                    original_message_id = email_obj.get("message_id") or None

                    if not to_address:
                        source_seen_ok = _mark_seen_in_poll_mailboxes(
                            uid,
                            also_flag=is_unique_case,
                            stop_event=stop_event,
                            source_mailbox=source_mailbox,
                            retries=4,
                        )
                        if not source_seen_ok:
                            print(
                                f"[Automata] Could not confirm \\Seen on source mailbox={source_mailbox} "
                                f"UID={uid}; stopping batch before next email."
                            )
                            break
                        continue

                    _assert_not_stopped(stop_event)
                    _save_reply_as_draft_and_mark_seen(
                        uid=uid,
                        source_mailbox=source_mailbox,
                        to_address=to_address,
                        reply_subject=reply_subject,
                        clean_body=clean_body,
                        original_message_id=original_message_id,
                        is_unique_case=is_unique_case,
                        stop_event=stop_event,
                    )
                    # Strict: immediately mark the original inbound as Seen,
                    # inside the per-message loop so it won't be retried.
                    _assert_not_stopped(stop_event)
                    source_seen_ok = _mark_seen_in_poll_mailboxes(
                        uid,
                        also_flag=is_unique_case,
                        stop_event=stop_event,
                        source_mailbox=source_mailbox,
                        retries=4,
                    )
                    if not source_seen_ok:
                        print(
                            f"[Automata] Could not confirm \\Seen on source mailbox={source_mailbox} "
                            f"UID={uid}; stopping batch before next email."
                        )
                        break
                    processed_email_ids.add(email_ref)
                    print(f"[Automata] Processed mailbox={source_mailbox} UID={uid} -> draft saved + marked Seen.")
                except InterruptedError:
                    print("[Automata] Stop requested during message processing, worker exiting.")
                    return
                except Exception as e:
                    print(f"[Automata] Failed to process UID={uid}: {e}")
                    # Best-effort: don't mark as seen if generation failed, so it can be retried later.
                    continue
        except InterruptedError:
            print("[Automata] Stop requested, worker exiting immediately.")
            return
        except Exception as e:
            print(f"[Automata] Worker loop error: {e}")

        _sleep_with_stop(POLL_INTERVAL_SECONDS, stop_event)

    print("[Automata] Worker stopped.")


_auto_load_saved_knowledge_base()

# --- Main layout (English SaaS UI) ---
st.title("🤖 Flow AI MailBot")
st.markdown("---")

if "settings_form_imap" not in st.session_state:
    imap_default = _get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default="")
    st.session_state["settings_form_imap"] = _normalize_imap_server_address(imap_default) if imap_default else ""
if "settings_form_gmail" not in st.session_state:
    st.session_state["settings_form_gmail"] = _get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default="")
if "primary_language" not in st.session_state:
    st.session_state["primary_language"] = _get_persisted_primary_language()
if "spoken_languages" not in st.session_state:
    st.session_state["spoken_languages"] = _get_persisted_spoken_languages()

with st.container():
    st.subheader(t("settings_panel"))
    with st.form("system_credentials_form"):
        imap_in = st.text_input(
            t("settings_imap"),
            value=str(st.session_state.get("settings_form_imap") or ""),
            placeholder=t("settings_imap_placeholder"),
            help=t("settings_imap_help"),
        )
        gmail_in = st.text_input(
            t("settings_gmail"),
            value=str(st.session_state.get("settings_form_gmail") or ""),
        )
        gmail_pw_in = st.text_input(
            t("settings_app_pw"),
            value=str(st.session_state.get("settings_form_app_password") or _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default="")),
            type="password",
        )
        openai_key_in = st.text_input(
            t("settings_openai"),
            value=str(st.session_state.get("settings_form_openai_key") or _get_config_value("OPENAI_API_KEY", default="")),
            type="password",
        )
        save_verify = st.form_submit_button(t("settings_save"), use_container_width=True)

    if save_verify:
        try:
            gmail_clean = (gmail_in or "").strip()
            pw_clean = (gmail_pw_in or "").strip()
            openai_clean = (openai_key_in or "").strip()
            if not (gmail_clean and pw_clean and openai_clean):
                raise ValueError("Gmail address, Gmail app password and OpenAI API key are required.")

            imap_clean = _normalize_imap_server_address(imap_in or IMAP_SERVER_DEFAULT)
            _verify_imap_login(imap_clean, gmail_clean, pw_clean)
            _verify_openai_api_key(openai_clean)
            primary_clean = str(st.session_state.get("primary_language") or _get_persisted_primary_language()).strip()
            spoken_clean = _coerce_spoken_languages_list(st.session_state.get("spoken_languages"))
            spoken_clean = [lang for lang in spoken_clean if lang in LANGUAGE_OPTIONS]
            if not spoken_clean:
                spoken_clean = _get_persisted_spoken_languages()
            if not primary_clean:
                primary_clean = _get_persisted_primary_language()

            _write_secrets_toml(
                imap_clean,
                gmail_clean,
                pw_clean,
                openai_clean,
                primary_clean,
                spoken_clean,
            )

            st.session_state["settings_form_imap"] = imap_clean
            st.session_state["settings_form_gmail"] = gmail_clean
            st.session_state["settings_form_app_password"] = pw_clean
            st.session_state["settings_form_openai_key"] = openai_clean
            _reload_runtime_credentials()
            st.session_state["system_credentials_verified"] = True
            st.session_state["_boot_credential_checked"] = True
            st.session_state.pop("boot_credential_error", None)

            st.success(t("settings_success"))
        except Exception as e:
            st.session_state["system_credentials_verified"] = False
            st.error(f"{t('settings_fail')} {e}")
    st.info("💡 Tip: To save credentials permanently so they survive page reloads and run in the background, add them to your Streamlit App's Advanced Settings -> Secrets.")

_ensure_boot_credential_state()
if st.session_state.get("boot_credential_error"):
    st.warning(f"{t('boot_verify_fail')} {st.session_state.get('boot_credential_error')}")

# --- Review panel: show original message + suggested reply ---
draft_for_ui = (
    st.session_state.get("editable_draft")
    or st.session_state.get("draft_response")
    or st.session_state.get("editable_reply")
    or st.session_state.get("draft_reply")
    or ""
)
selection_for_ui = st.session_state.get("last_selection") or {}
if draft_for_ui:
    marker = "--- Original Message ---"
    original_for_ui = ""
    suggested_for_ui = str(draft_for_ui)
    if marker in suggested_for_ui:
        suggested_for_ui, original_for_ui = suggested_for_ui.split(marker, 1)
        suggested_for_ui = suggested_for_ui.rstrip()
        original_for_ui = original_for_ui.strip()

    if not original_for_ui:
        latest_email = st.session_state.get("latest_email") or {}
        original_for_ui = str(latest_email.get("body") or "").strip()

    with st.expander(t("original_message_section"), expanded=True):
        st.text_area(
            t("original_message_label"),
            value=original_for_ui or t("no_original_message"),
            height=200,
            disabled=True,
            key="ui_original_message_preview",
        )

    category_for_ui = str(selection_for_ui.get("category") or "").strip()
    st.caption(f"{t('category_label')}: {category_for_ui or '-'}")
    st.subheader(t("suggested_reply_header"))
    st.text_area(
        t("suggested_reply_label"),
        value=suggested_for_ui.strip() or str(draft_for_ui).strip(),
        height=260,
        disabled=True,
        key="ui_suggested_reply_preview",
    )

with st.sidebar:
    st.subheader("Workspace")
    st.header(t("kb_header"))

    # Background automation controls (pinned at top of sidebar)
    persisted_run_flag = _read_automation_status_flag()
    if "automation_should_run" not in st.session_state:
        st.session_state["automation_should_run"] = persisted_run_flag == "RUNNING"
    if "automation_stop_requested" not in st.session_state:
        st.session_state["automation_stop_requested"] = False
    if "automata_running" not in st.session_state:
        st.session_state["automata_running"] = False
    if "worker_thread" not in st.session_state:
        st.session_state["worker_thread"] = None
    if "worker_stop_event" not in st.session_state:
        st.session_state["worker_stop_event"] = None
    if "primary_language" not in st.session_state:
        st.session_state["primary_language"] = _get_persisted_primary_language()
    if "spoken_languages" not in st.session_state:
        st.session_state["spoken_languages"] = _get_persisted_spoken_languages()

    cred_ok = bool(st.session_state.get("system_credentials_verified"))
    should_run = bool(st.session_state.get("automation_should_run"))
    stop_requested = bool(st.session_state.get("automation_stop_requested"))
    worker_thread = st.session_state.get("worker_thread")
    worker_alive = bool(worker_thread is not None and getattr(worker_thread, "is_alive", lambda: False)())

    # Reconcile desired state vs actual worker state on every rerun.
    # IMPORTANT: only an explicit Stop click should request shutdown.
    if stop_requested:
        ev = st.session_state.get("worker_stop_event")
        if worker_alive and ev is not None and not ev.is_set():
            ev.set()
        st.session_state["automation_should_run"] = False
        if not worker_alive:
            st.session_state["automation_stop_requested"] = False
        st.session_state["automata_running"] = worker_alive
    elif should_run:
        if not worker_alive:
            primary_language = _get_persisted_primary_language()
            spoken_languages = _normalize_spoken_languages_for_prompt(_get_persisted_spoken_languages())

            stop_event = threading.Event()
            st.session_state["worker_stop_event"] = stop_event
            new_thread = threading.Thread(
                target=_automata_worker_loop,
                args=(primary_language, spoken_languages, stop_event),
                daemon=True,
            )
            st.session_state["worker_thread"] = new_thread
            new_thread.start()
            worker_thread = new_thread
            worker_alive = True
        st.session_state["automata_running"] = worker_alive
    else:
        # Defensive: if rerender/state glitch temporarily drops should_run while the
        # worker is alive, keep it running instead of stopping unexpectedly.
        if worker_alive:
            st.session_state["automation_should_run"] = True
            st.session_state["automata_running"] = True
        else:
            st.session_state["automata_running"] = False

    running = persisted_run_flag == "RUNNING"
    active_primary_color = "#dc2626" if running else "#16a34a"
    active_primary_hover = "#b91c1c" if running else "#15803d"
    st.markdown(
        f"""
        <style>
        section[data-testid="stSidebar"] div.stButton > button {{
            background-color: #9ca3af;
            color: #ffffff;
            border: 1px solid #9ca3af;
        }}
        section[data-testid="stSidebar"] div.stButton > button:hover:not(:disabled) {{
            background-color: #6b7280;
            border-color: #6b7280;
            color: #ffffff;
        }}
        section[data-testid="stSidebar"] div.stButton > button[kind="primary"] {{
            background-color: {active_primary_color};
            border-color: {active_primary_color};
            color: #ffffff;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(t("automata_status_running") if running else t("automata_status_stopped"))
    if not cred_ok and not running:
        st.caption(t("automata_need_verify"))

    # Explicit controls: stable across rerenders.
    col_start, col_stop = st.columns(2)
    start_clicked = col_start.button(
        t("automata_start_btn"),
        type="primary" if not running else "secondary",
        use_container_width=True,
        disabled=(not cred_ok) or running,
    )
    stop_clicked = col_stop.button(
        t("automata_stop_btn"),
        type="primary" if running else "secondary",
        use_container_width=True,
        disabled=not running,
    )

    if start_clicked:
        primary_language_input = _get_persisted_primary_language()
        kb_current = st.session_state.get("kb_df")
        if kb_current is None or getattr(kb_current, "empty", True):
            st.session_state["automation_should_run"] = False
            st.error("Please upload the Knowledge Base (Excel) before starting!")
        elif not primary_language_input:
            st.session_state["automation_should_run"] = False
            st.error("Error: Primary language is required to start the automation.")
        else:
            _persist_language_settings_to_toml()
            _write_automation_status_flag("RUNNING")
            st.session_state["automation_should_run"] = True
            st.session_state["automation_stop_requested"] = False
            st.rerun()

    if stop_clicked:
        _write_automation_status_flag("STOPPED")
        st.session_state["automation_should_run"] = False
        st.session_state["automation_stop_requested"] = True
        ev = st.session_state.get("worker_stop_event")
        if ev is not None:
            ev.set()
        st.rerun()

    # Language settings (strict persisted defaults from TOML/secrets).
    persisted_primary = _get_persisted_primary_language()
    persisted_spoken = _get_persisted_spoken_languages()

    primary_current = str(st.session_state.get("primary_language") or "").strip()
    if primary_current not in LANGUAGE_OPTIONS:
        primary_current = persisted_primary
        st.session_state["primary_language"] = primary_current
    try:
        primary_index = LANGUAGE_OPTIONS.index(primary_current)
    except ValueError:
        primary_index = 0

    spoken_current = _coerce_spoken_languages_list(st.session_state.get("spoken_languages"))
    spoken_current = [lang for lang in spoken_current if lang in LANGUAGE_OPTIONS]
    if not spoken_current:
        spoken_current = [lang for lang in persisted_spoken if lang in LANGUAGE_OPTIONS]
        st.session_state["spoken_languages"] = spoken_current

    st.selectbox(
        t("primary_lang"),
        options=LANGUAGE_OPTIONS,
        index=primary_index,
        placeholder="Select primary language",
        key="primary_language",
        on_change=_persist_language_settings_to_toml,
    )
    st.multiselect(
        t("spoken_langs"),
        options=LANGUAGE_OPTIONS,
        default=spoken_current,
        key="spoken_languages",
        on_change=_persist_language_settings_to_toml,
    )

    sidebar_openai_key = _get_server_openai_api_key()
    if not sidebar_openai_key or sidebar_openai_key in {"", "ide-masold-be-a-kulcsot"}:
        st.warning(t("openai_missing"))

    if st.session_state.get("kb_df") is not None:
        # Show original filename if available.
        orig_name = None
        try:
            if SAVED_KB_ORIG_NAME_PATH.exists():
                orig_name = (SAVED_KB_ORIG_NAME_PATH.read_text(encoding="utf-8") or "").strip()
        except Exception:
            orig_name = None

        shown = (orig_name or st.session_state.get("kb_file_name") or "").strip()
        if shown:
            st.info(f"{t('active_db')} {shown}")
        else:
            st.info(t("active_kb_loaded"))

        if st.button(t("delete_db_btn"), use_container_width=True, type="secondary"):
            try:
                if SAVED_KB_XLSX_PATH.exists():
                    SAVED_KB_XLSX_PATH.unlink()
                if SAVED_KB_ORIG_NAME_PATH.exists():
                    SAVED_KB_ORIG_NAME_PATH.unlink()
            except Exception as e:
                st.error(f"Could not delete saved files: {e}")
            else:
                for k in (
                    "kb_df",
                    "kb_file_bytes",
                    "kb_file_name",
                    "kb_loaded",
                    "kb_shape",
                    "last_selection",
                    "draft_response",
                    "editable_draft",
                    "draft_reply",
                    "editable_reply",
                    "is_unique_case",
                ):
                    st.session_state.pop(k, None)
                st.success(t("delete_db_ok"))
                st.rerun()
    elif SAVED_KB_XLSX_PATH.exists():
        if st.button(t("delete_db_btn"), use_container_width=True, type="secondary"):
            try:
                SAVED_KB_XLSX_PATH.unlink()
                if SAVED_KB_ORIG_NAME_PATH.exists():
                    SAVED_KB_ORIG_NAME_PATH.unlink()
            except Exception as e:
                st.error(f"Could not delete saved files: {e}")
            else:
                for k in (
                    "kb_df",
                    "kb_file_bytes",
                    "kb_file_name",
                    "kb_loaded",
                    "kb_shape",
                    "last_selection",
                    "draft_response",
                    "editable_draft",
                    "draft_reply",
                    "editable_reply",
                    "is_unique_case",
                ):
                    st.session_state.pop(k, None)
                st.success(t("delete_db_ok"))
                st.rerun()

    uploaded = st.file_uploader(
        t("upload_new"),
        type=["xlsx"],
        accept_multiple_files=False,
        help=t("upload_help"),
        key="kb_uploader",
    )

    if uploaded is not None:
        try:
            kb_bytes = uploaded.getvalue()
            st.session_state["kb_file_name"] = uploaded.name
            st.session_state["kb_file_bytes"] = kb_bytes

            # Persistence: always save as saved_knowledge_base.xlsx in project root.
            try:
                SAVED_KB_XLSX_PATH.write_bytes(kb_bytes)
                try:
                    SAVED_KB_ORIG_NAME_PATH.write_text(uploaded.name, encoding="utf-8")
                except Exception:
                    pass
            except Exception as e:
                st.error(f"{t('save_kb_failed')} {e}")

            kb_df = _normalize_columns(_read_knowledge_base_from_bytes(uploaded.name, kb_bytes))
            # Source of truth: keep the uploaded dataframe in session_state
            st.session_state["kb_df"] = kb_df
            st.session_state["kb_loaded"] = True
            st.session_state["kb_shape"] = (int(len(kb_df)), int(len(kb_df.columns)))
        except Exception as e:
            st.session_state.pop("kb_df", None)
            st.session_state["kb_loaded"] = False
            st.session_state["kb_shape"] = (0, 0)
            st.error(str(e))
    else:
        pass




