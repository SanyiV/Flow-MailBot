import os
from pathlib import Path
import tomllib

import pandas as pd
import streamlit as st

APP_ROOT = Path(__file__).resolve().parent
SECRETS_TOML_PATH = APP_ROOT / "hotel_credentials.toml"
AUTOMATION_STATUS_PATH = APP_ROOT / "automation_status.txt"
SAVED_KB_XLSX_PATH = APP_ROOT / "saved_knowledge_base.xlsx"
SAVED_KB_ORIG_NAME_PATH = APP_ROOT / "saved_filename.txt"

IMAP_SERVER_DEFAULT = "imap.gmail.com"
PRIMARY_LANGUAGE_DEFAULT = "English"
LANGUAGE_OPTIONS = [
    "Afrikaans", "Albanian", "Amharic", "Arabic", "Armenian", "Azerbaijani", "Basque", "Bengali",
    "Bosnian", "Bulgarian", "Burmese", "Catalan", "Chinese (Cantonese)", "Chinese (Mandarin)",
    "Croatian", "Czech", "Danish", "Dutch", "English (Australia)", "English (UK)", "English (US)",
    "Estonian", "Filipino", "Finnish", "French (Canada)", "French (France)", "Galician", "Georgian",
    "German", "Greek", "Gujarati", "Hebrew", "Hindi", "Hungarian", "Icelandic", "Indonesian", "Irish",
    "Italian", "Japanese", "Kannada", "Kazakh", "Khmer", "Korean", "Latvian", "Lithuanian",
    "Macedonian", "Malay", "Malayalam", "Maltese", "Marathi", "Mongolian", "Nepali", "Norwegian",
    "Persian (Farsi)", "Polish", "Portuguese (Brazil)", "Portuguese (Portugal)", "Punjabi", "Romanian",
    "Russian", "Serbian", "Sinhala", "Slovak", "Slovenian", "Spanish (Latin America)", "Spanish (Spain)",
    "Swahili", "Swedish", "Tamil", "Telugu", "Thai", "Turkish", "Ukrainian", "Urdu", "Uzbek",
    "Vietnamese", "Welsh", "Zulu",
]


def _read_credentials_toml() -> dict:
    if not SECRETS_TOML_PATH.exists():
        return {}
    try:
        data = tomllib.loads(SECRETS_TOML_PATH.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _get_streamlit_secret(key: str) -> str | None:
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


def _get_config_value(*keys: str, default: str = "") -> str:
    cfg = _read_credentials_toml()
    for k in keys:
        v = str(cfg.get(k) or "").strip()
        if v:
            return v
    for k in keys:
        v2 = _get_streamlit_secret(k)
        if v2:
            return v2
    for k in keys:
        v3 = (os.getenv(k) or "").strip()
        if v3:
            return v3
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

    cfg = _read_credentials_toml()
    for k in keys:
        vals = _coerce_list(cfg.get(k))
        if vals:
            return vals
    for k in keys:
        vals = _coerce_list(_get_streamlit_secret_raw(k))
        if vals:
            return vals
    for k in keys:
        vals = _coerce_list(os.getenv(k))
        if vals:
            return vals
    return []


def _toml_escape_double_quoted(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace('"', '\\"')


def _toml_string_array(values: list[str]) -> str:
    out = [f'"{_toml_escape_double_quoted(v)}"' for v in values if str(v).strip()]
    return "[" + ", ".join(out) + "]"


def _read_optional_draft_secret_lines() -> str:
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


def _write_credentials_toml(
    imap_server: str,
    gmail_user: str,
    gmail_password: str,
    openai_api_key: str,
    primary_language: str,
    spoken_languages: list[str],
) -> None:
    SECRETS_TOML_PATH.parent.mkdir(parents=True, exist_ok=True)
    spoken_clean = [str(x).strip() for x in spoken_languages if str(x).strip()]
    body = (
        "## Managed by AI Complaint Handler — do not commit real secrets to public repositories.\n"
        f'OPENAI_API_KEY = "{_toml_escape_double_quoted(openai_api_key)}"\n'
        f'IMAP_SERVER = "{_toml_escape_double_quoted(imap_server or IMAP_SERVER_DEFAULT)}"\n'
        f'GMAIL_USER = "{_toml_escape_double_quoted(gmail_user)}"\n'
        f'GMAIL_PASSWORD = "{_toml_escape_double_quoted(gmail_password)}"\n'
        f'PRIMARY_LANGUAGE = "{_toml_escape_double_quoted(primary_language)}"\n'
        f"SPOKEN_LANGUAGES = {_toml_string_array(spoken_clean)}\n"
        f"{_read_optional_draft_secret_lines()}"
    )
    SECRETS_TOML_PATH.write_text(body, encoding="utf-8")


def _read_automation_status_flag() -> str:
    if not AUTOMATION_STATUS_PATH.exists():
        return "STOPPED"
    try:
        val = (AUTOMATION_STATUS_PATH.read_text(encoding="utf-8", errors="ignore") or "").strip().upper()
    except Exception:
        return "STOPPED"
    return val if val in {"RUNNING", "STOPPED"} else "STOPPED"


def _write_automation_status_flag(status: str) -> None:
    val = (status or "").strip().upper()
    if val not in {"RUNNING", "STOPPED"}:
        return
    AUTOMATION_STATUS_PATH.write_text(val, encoding="utf-8")


def _persisted_primary() -> str:
    p = (_get_config_value("PRIMARY_LANGUAGE", default=PRIMARY_LANGUAGE_DEFAULT) or "").strip()
    return p if p in LANGUAGE_OPTIONS else PRIMARY_LANGUAGE_DEFAULT


def _persisted_spoken() -> list[str]:
    vals = [x for x in _get_config_list("SPOKEN_LANGUAGES") if x in LANGUAGE_OPTIONS]
    return vals if vals else [PRIMARY_LANGUAGE_DEFAULT]


def _persist_language_settings_only() -> None:
    primary = str(st.session_state.get("primary_language") or "").strip()
    spoken = [x for x in st.session_state.get("spoken_languages", []) if x in LANGUAGE_OPTIONS]
    if primary not in LANGUAGE_OPTIONS:
        return
    _write_credentials_toml(
        _get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT),
        _get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default=""),
        _get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default=""),
        _get_config_value("OPENAI_API_KEY", default=""),
        primary,
        spoken,
    )


st.set_page_config(page_title="Flow AI MailBot", page_icon="🤖", layout="wide")
st.title("🤖 Flow AI MailBot")
st.markdown("---")

if "settings_form_imap" not in st.session_state:
    st.session_state["settings_form_imap"] = _get_config_value("IMAP_SERVER", "IMAP_SERVER_ADDRESS", "IMAP_HOST", default=IMAP_SERVER_DEFAULT)
if "settings_form_gmail" not in st.session_state:
    st.session_state["settings_form_gmail"] = _get_config_value("GMAIL_USER", "EMAIL_ADDRESS", default="")
if "primary_language" not in st.session_state:
    st.session_state["primary_language"] = _persisted_primary()
if "spoken_languages" not in st.session_state:
    st.session_state["spoken_languages"] = _persisted_spoken()

with st.container():
    st.subheader("System Settings & Credentials")
    with st.form("system_credentials_form"):
        imap_in = st.text_input("IMAP server address", value=str(st.session_state.get("settings_form_imap") or ""), placeholder="e.g., imap.gmail.com")
        gmail_in = st.text_input("Email Address", value=str(st.session_state.get("settings_form_gmail") or ""))
        gmail_pw_in = st.text_input("App Password", value=_get_config_value("GMAIL_PASSWORD", "EMAIL_APP_PASSWORD", default=""), type="password")
        openai_key_in = st.text_input("OpenAI API Key", value=_get_config_value("OPENAI_API_KEY", default=""), type="password")
        save_verify = st.form_submit_button("Save & Verify", use_container_width=True)

    if save_verify:
        try:
            primary_clean = str(st.session_state.get("primary_language") or _persisted_primary()).strip()
            spoken_clean = [x for x in st.session_state.get("spoken_languages", []) if x in LANGUAGE_OPTIONS]
            _write_credentials_toml(
                imap_in.strip() or IMAP_SERVER_DEFAULT,
                (gmail_in or "").strip(),
                (gmail_pw_in or "").strip(),
                (openai_key_in or "").strip(),
                primary_clean,
                spoken_clean,
            )
            st.session_state["settings_form_imap"] = imap_in.strip() or IMAP_SERVER_DEFAULT
            st.session_state["settings_form_gmail"] = (gmail_in or "").strip()
            st.success("Credentials verified and saved to `hotel_credentials.toml`.")
        except Exception as e:
            st.error(f"Verification or save failed. {e}")
    st.info("💡 Tip: To save credentials permanently so they survive page reloads and run in the background, add them to your Streamlit App's Advanced Settings -> Secrets.")

with st.sidebar:
    st.subheader("Workspace")
    st.header("Knowledge base")

    running = _read_automation_status_flag() == "RUNNING"
    st.markdown("Automation: :green[**RUNNING**]" if running else "Automation: :red[**STOPPED**]")

    col_start, col_stop = st.columns(2)
    start_clicked = col_start.button("Start automation", use_container_width=True, disabled=running)
    stop_clicked = col_stop.button("Stop automation", use_container_width=True, disabled=not running)
    if start_clicked:
        _persist_language_settings_only()
        _write_automation_status_flag("RUNNING")
        st.rerun()
    if stop_clicked:
        _write_automation_status_flag("STOPPED")
        st.rerun()

    persisted_primary = _persisted_primary()
    persisted_spoken = _persisted_spoken()
    current_primary = str(st.session_state.get("primary_language") or "").strip()
    if current_primary not in LANGUAGE_OPTIONS:
        current_primary = persisted_primary
        st.session_state["primary_language"] = current_primary
    try:
        primary_index = LANGUAGE_OPTIONS.index(current_primary)
    except ValueError:
        primary_index = 0
    current_spoken = [x for x in st.session_state.get("spoken_languages", []) if x in LANGUAGE_OPTIONS]
    if not current_spoken:
        current_spoken = [x for x in persisted_spoken if x in LANGUAGE_OPTIONS]
        st.session_state["spoken_languages"] = current_spoken

    st.selectbox(
        "Primary language (for internal translations)",
        options=LANGUAGE_OPTIONS,
        index=primary_index,
        key="primary_language",
        on_change=_persist_language_settings_only,
    )
    st.multiselect(
        "Spoken languages (comma-separated)",
        options=LANGUAGE_OPTIONS,
        default=current_spoken,
        key="spoken_languages",
        on_change=_persist_language_settings_only,
    )

    if SAVED_KB_XLSX_PATH.exists():
        shown = ""
        try:
            if SAVED_KB_ORIG_NAME_PATH.exists():
                shown = (SAVED_KB_ORIG_NAME_PATH.read_text(encoding="utf-8") or "").strip()
        except Exception:
            shown = ""
        st.info(f"Active database: {shown or SAVED_KB_XLSX_PATH.name}")
        if st.button("Delete active database", use_container_width=True, type="secondary"):
            try:
                SAVED_KB_XLSX_PATH.unlink(missing_ok=True)
                SAVED_KB_ORIG_NAME_PATH.unlink(missing_ok=True)
                st.success("Saved knowledge base removed. You can upload a new file.")
                st.rerun()
            except Exception as e:
                st.error(f"Could not delete saved files: {e}")

    uploaded = st.file_uploader(
        "Upload Master Database (Excel)",
        type=["xlsx"],
        accept_multiple_files=False,
        help="Uploading overwrites the saved knowledge base on disk.",
    )
    if uploaded is not None:
        try:
            kb_bytes = uploaded.getvalue()
            SAVED_KB_XLSX_PATH.write_bytes(kb_bytes)
            SAVED_KB_ORIG_NAME_PATH.write_text(uploaded.name, encoding="utf-8")
            df = pd.read_excel(SAVED_KB_XLSX_PATH)
            st.success(f"Knowledge base uploaded: {uploaded.name} ({len(df)} rows).")
        except Exception as e:
            st.error(f"Could not save the knowledge base to disk: {e}")

st.caption("Backend daemon runs from `motor.py`. Start it separately (e.g. `python motor.py`).")
