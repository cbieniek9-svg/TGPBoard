import streamlit as st
import pandas as pd
import uuid
import html
import io
import logging
import traceback
import bcrypt
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("tgp_ops")

# -------------------------
# TIMEZONE & HELPERS (EDMONTON LOCK)
# -------------------------
try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Edmonton")
except ImportError:
    import pytz
    LOCAL_TZ = pytz.timezone("America/Edmonton")

def yeg_now() -> datetime:
    return datetime.now(LOCAL_TZ)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def gen_id() -> str:
    return uuid.uuid4().hex

def hash_pin(pin: str) -> str:
    return bcrypt.hashpw(str(pin).encode(), bcrypt.gensalt()).decode()

def verify_pin(pin: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(str(pin).encode(), hashed.encode())
    except Exception:
        return False

# -------------------------
# CONSTANTS
# -------------------------
VENDOR_SCHEDULE: dict = {
    "Monday":    ["Old Dutch", "Coke", "Pepsi", "Frito Lay (Retail)", "Frito Lay (Vending)", "Italian Bakery", "Canada Bread"],
    "Tuesday":   ["TGP", "Old Dutch", "Kenelli", "Frito Lay (Retail)"],
    "Wednesday": ["Old Dutch", "Frito Lay (Retail)"],
    "Thursday":  ["TGP", "Old Dutch", "Pepsi", "Frito Lay (Vending)", "Italian Bakery", "Canada Bread"],
    "Friday":    ["Old Dutch", "Coke", "Frito Lay (Retail)"],
    "Saturday":  ["Italian Bakery"],
    "Sunday":    ["TGP"],
}

PREMIUM_STAFF:         list  = ["Chris", "Ashley", "Luke", "Chandler"]
ORDER_LOCATIONS:       list  = ["1", "2", "3", "22"]
AISLES:                list  = ["Aisle 1", "Aisle 2", "Aisle 3", "Aisle 4", "Aisle 5",
                                 "Aisle 6", "Aisle 7", "Aisle 8", "Receiving", "Freezer", "Bakery", "Outside"]
PRIORITY_URGENT_WORDS: tuple = ("urgent", "spill", "fire", "code", "now", "rush")
PRIORITY_HIGH_WORDS:   tuple = ("high", "fast", "soon", "bale", "sweep")
WINTER_MONTHS:         tuple = (11, 12, 1, 2, 3)

# -------------------------
# PAGE CONFIG & SESSION DEFAULTS
# -------------------------
st.set_page_config(page_title="TGP Operations", layout="wide", initial_sidebar_state="expanded")

for _key in ["hidden_t", "hidden_o", "hidden_s", "hidden_e"]:
    if _key not in st.session_state:
        st.session_state[_key] = []
    if len(st.session_state[_key]) > 100:
        st.session_state[_key] = st.session_state[_key][-50:]

if "write_errors" not in st.session_state:
    st.session_state["write_errors"] = []

# -------------------------
# ERROR TRACKING & VALIDATION
# -------------------------
def _log_error(context: str, exc: Exception) -> None:
    msg = f"[{utc_now_iso()}] {context}: {exc}\n{traceback.format_exc()}"
    logger.error(msg)
    st.session_state["write_errors"].append(msg)
    if len(st.session_state["write_errors"]) > 20:
        st.session_state["write_errors"] = st.session_state["write_errors"][-20:]

def _safe_remove(lst: list, value: str) -> None:
    try:
        lst.remove(value)
    except ValueError:
        pass

def sanitize_input(text: str, max_len: int = 500, allow_empty: bool = False) -> str:
    text = str(text).strip()
    if not allow_empty and not text:
        raise ValueError("Input cannot be empty")
    if len(text) > max_len:
        raise ValueError(f"Input exceeds {max_len} characters (got {len(text)})")
    return html.escape(text)

def validate_integer(value, min_val: int = 0, max_val: int = None) -> int:
    try:
        val = int(value)
        if val < min_val:
            raise ValueError(f"Value must be >= {min_val}")
        if max_val and val > max_val:
            raise ValueError(f"Value must be <= {max_val}")
        return val
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid integer: {e}")

# -------------------------
# URL FLAGS
# -------------------------
query_params = st.query_params

def is_flag_active(key: str) -> bool:
    if key in query_params:
        return str(query_params.get(key, "")).lower() not in ("false", "0", "no")
    return False

is_tv_url_mode      = is_flag_active("tvmode")
is_tv_settings_mode = is_flag_active("settings")
is_cs_mode          = str(query_params.get("mode", "")).lower() in ("cs", "desk", "service")

if "should_auto_refresh" not in st.session_state:
    st.session_state["should_auto_refresh"] = is_tv_url_mode

# -------------------------
# SUPABASE CLIENT (CACHED)
# -------------------------
@st.cache_resource
def get_supabase_client() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

try:
    supabase = get_supabase_client()
except Exception as _e:
    st.error("Missing SUPABASE_URL or SUPABASE_KEY in secrets.")
    st.stop()

def to_df(data: list) -> pd.DataFrame:
    df = pd.DataFrame(data)
    if not df.empty:
        df.columns = df.columns.str.lower()
    return df

# -------------------------
# FRAGMENTED CACHING
# -------------------------
@st.cache_data(ttl=2)
def load_fast_data() -> dict:
    tasks    = to_df(supabase.table("tasks").select("*").eq("status", "Open").execute().data)
    oos      = to_df(supabase.table("oos").select("*").eq("status", "Open").execute().data)
    orders   = to_df(supabase.table("special_orders").select("*").eq("status", "Open").execute().data)
    expected = to_df(supabase.table("expected_orders").select("*").eq("status", "Pending").execute().data)

    t_res = (supabase.table("tasks")
             .select("task_id, task_detail, time_closed, closed_by")
             .eq("status", "Closed").neq("closed_by", "AUTO")
             .order("time_closed", desc=True).limit(6).execute())
    o_res = (supabase.table("oos")
             .select("oos_id, zone, time_closed, closed_by")
             .eq("status", "Closed").neq("closed_by", "AUTO")
             .order("time_closed", desc=True).limit(6).execute())

    audits = []
    for t in t_res.data:
        if t.get("time_closed"):
            audits.append({"id": t.get("task_id"), "event": f"Task: {t.get('task_detail')}",
                           "time": t.get("time_closed"), "user": t.get("closed_by"), "type": "task"})
    for o in o_res.data:
        if o.get("time_closed"):
            audits.append({"id": o.get("oos_id"), "event": f"Cleared Holes: {o.get('zone')}",
                           "time": o.get("time_closed"), "user": o.get("closed_by"), "type": "oos"})

    a_df = pd.DataFrame(audits)
    if not a_df.empty:
        a_df = a_df.sort_values(by="time", ascending=False).head(6)

    return {"tasks": tasks, "oos": oos, "orders": orders, "expected": expected, "audit": a_df}

@st.cache_data(ttl=30)
def load_slow_data() -> dict:
    return {
        "counts":   to_df(supabase.table("counts").select("*").eq("id", 1).execute().data),
        "staff":    to_df(supabase.table("staff").select("*").execute().data),
        "settings": to_df(supabase.table("settings").select("*").execute().data),
        "ticker":   to_df(supabase.table("ticker").select("*").execute().data),
    }

def load_historical_data(target_date_str: str | None = None) -> dict:
    empty = {"tasks": pd.DataFrame(), "oos": pd.DataFrame()}
    try:
        if target_date_str:
            start_iso = f"{target_date_str}T00:00:00Z"
            end_iso   = f"{target_date_str}T23:59:59Z"
            tasks = to_df(supabase.table("tasks").select("*").in_("status", ["Closed", "Archived"])
                          .gte("time_closed", start_iso).lte("time_closed", end_iso).execute().data)
            oos   = to_df(supabase.table("oos").select("*").in_("status", ["Closed", "Archived"])
                          .gte("time_closed", start_iso).lte("time_closed", end_iso).execute().data)
        else:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            tasks = to_df(supabase.table("tasks").select("*").in_("status", ["Closed", "Archived"])
                          .gte("time_closed", cutoff).execute().data)
            oos   = to_df(supabase.table("oos").select("*").in_("status", ["Closed", "Archived"])
                          .gte("time_closed", cutoff).execute().data)

        if not tasks.empty:
            tasks["time_submitted"] = pd.to_datetime(tasks["time_submitted"], errors="coerce")
            tasks["time_closed"]    = pd.to_datetime(tasks["time_closed"],    errors="coerce")
            tasks["actual_mins"]    = (tasks["time_closed"] - tasks["time_submitted"]).dt.total_seconds() / 60.0

        return {"tasks": tasks, "oos": oos}
    except Exception as e:
        _log_error("load_historical_data", e)
        return empty

def clear_fast_cache() -> None:
    load_fast_data.clear()

def clear_full_cache() -> None:
    load_fast_data.clear()
    load_slow_data.clear()

# -------------------------
# LOAD DATA
# -------------------------
fast_data = load_fast_data()
slow_data = load_slow_data()

c_df     = slow_data["counts"]
staff_df = slow_data["staff"]
set_df   = slow_data["settings"]

g_pcs       = int(c_df["grocery"].iloc[0]) if not c_df.empty else 0
f_pcs       = int(c_df["frozen"].iloc[0])  if not c_df.empty else 0
staff_count = max(1, int(c_df["staff"].iloc[0])) if not c_df.empty else 1

master_staff = (staff_df[staff_df["name"] != "Unassigned"]["name"].tolist()
                if not staff_df.empty else PREMIUM_STAFF)
active_staff = (staff_df[(staff_df["active"] == 1) & (staff_df["name"] != "Unassigned")]["name"].tolist()
                if not staff_df.empty else PREMIUM_STAFF)

# --- Settings defaults ---
font_scale        = 100
admin_pin_hash    = ""
cases_per_hour    = 55.0
start_time_str    = "07:00"
end_time_str      = "15:00"
shift_notes       = ""
is_critical_alert = False
seasonal_progress = 0

if not set_df.empty:
    def _get_setting(name: str, default):
        v = set_df.loc[set_df["setting_name"] == name, "setting_value"]
        return v.iloc[0] if not v.empty else default

    admin_pin_hash    = str(_get_setting("Admin_PIN",         ""))
    cases_per_hour    = float(_get_setting("Cases_Per_Hour",  55.0))
    start_time_str    = str(_get_setting("Start_Time",        "07:00"))
    end_time_str      = str(_get_setting("End_Time",          "15:00"))
    shift_notes       = str(_get_setting("Shift_Notes",       ""))
    is_critical_alert = str(_get_setting("Critical_Alert",    "0")) == "1"
    seasonal_progress = int(_get_setting("Seasonal_Progress", 0))
    font_scale        = int(float(_get_setting("TV_Scale",    100)))

font_scale = max(70, min(200, font_scale))

def _safe_parse_time(time_str: str, fallback: str) -> str:
    try:
        datetime.strptime(time_str, "%H:%M")
        return time_str
    except ValueError:
        return fallback

start_time_str = _safe_parse_time(start_time_str, "07:00")
end_time_str   = _safe_parse_time(end_time_str,   "15:00")

# -------------------------
# CSS INJECTION
# -------------------------
st.markdown(f"""
<style>
footer {{ visibility: hidden; }}
#MainMenu {{ visibility: hidden; }}

html, body, .stApp {{
    font-size: {font_scale}%;
    background-color: #000000;
    color: #88ccff;
    font-family: 'Arial Narrow', 'Arial', sans-serif;
    overflow-x: hidden;
    text-transform: uppercase;
}}

@media screen and (min-width: 1025px) {{
    header[data-testid="stHeader"] {{ visibility: hidden; }}
    .block-container {{ padding-top: 0.5rem; padding-bottom: 3rem; padding-left: 1rem; padding-right: 1rem; max-width: 98vw !important; margin: auto; }}
}}
@media screen and (max-width: 1024px) {{
    header[data-testid="stHeader"] {{ visibility: visible !important; background-color: #000000; }}
    .block-container {{ padding-top: 3rem; padding-bottom: 5rem; padding-left: 0.5rem; padding-right: 0.5rem; max-width: 100vw !important; }}
}}

div[data-testid="column"] {{ min-width: 0 !important; }}
div[data-testid="stVerticalBlock"] {{ gap: 0.4rem !important; }}

.header-bar {{ display: flex; align-items: flex-end; justify-content: space-between; border-bottom: 4px solid #ffaa00; margin-bottom: 15px; padding-bottom: 8px; padding-top: 10px; position: relative; }}
.header-bar::before {{ content: ''; position: absolute; left: 0; bottom: -4px; width: 60px; height: 20px; background: #ffaa00; border-radius: 10px 0 0 10px; }}
.header-title {{ font-size: 2.2em; font-weight: 300; color: #eef5ff; letter-spacing: 5px; margin: 0 0 0 75px; line-height: 0.9; }}
.header-time {{ color: #88ccff; font-size: 1.3em; font-weight: 400; margin: 0; letter-spacing: 2px; }}

.alert-banner {{ background: #ff3333; color: #ffffff; padding: 10px 20px; font-weight: bold; border-radius: 5px; margin-bottom: 15px; border-left: 10px solid #990000; letter-spacing: 2px; }}
.shift-note {{ background: rgba(31, 59, 92, 0.4); border-left: 4px solid #00e5ff; padding: 10px 15px; margin-bottom: 15px; color: #eef5ff; font-size: 1.1em; }}

.kpi-container {{ display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: 10px; margin-bottom: 20px; width: 100%; }}
.kpi-box {{ background: rgba(11, 26, 46, 0.6); border-right: 5px solid #00e5ff; padding: 10px 15px; border-radius: 15px 0 0 15px; display: flex; flex-direction: column; justify-content: center; }}
.kpi-box.urgent {{ border-right-color: #ff3333; background: rgba(42, 10, 10, 0.6); }}
.kpi-box.amber  {{ border-right-color: #ffaa00; background: rgba(42, 31, 10, 0.6); }}
.kpi-label {{ font-size: 0.8em; font-weight: 700; color: #6699cc; letter-spacing: 2px; margin-bottom: 2px; white-space: nowrap; }}
.kpi-value {{ font-size: 1.6em; font-weight: 300; color: #ffffff; white-space: nowrap; line-height: 1; }}

.data-card {{ background: rgba(11, 26, 46, 0.5); border-left: 6px solid #00e5ff; padding: 12px 15px; margin-bottom: 8px; border-radius: 0 20px 20px 0; font-size: 1em; line-height: 1.4; display: flex; justify-content: space-between; align-items: center; letter-spacing: 1px; }}
.data-urgent {{ border-left-color: #ff3333; background: rgba(42, 10, 10, 0.5); color: #ffcccc; }}
.data-high   {{ border-left-color: #ffaa00; background: rgba(42, 31, 10, 0.5); color: #ffebcc; }}
.card-zone {{ font-weight: 700; color: inherit; letter-spacing: 2px; margin-right: 8px; font-size: 0.9em; opacity: 0.8; }}
.card-meta {{ font-size: 0.75em; text-align: right; color: #6699cc; letter-spacing: 1px; }}
.card-meta strong {{ color: #00e5ff; font-size: 1.1em; }}

.terminal-box {{ background: rgba(10, 5, 20, 0.8); border-left: 3px solid #a855f7; border-radius: 0 10px 10px 0; padding: 12px; font-family: 'Courier New', monospace; font-size: 0.85em; color: #9ca3af; margin-top: 15px; text-transform: none; }}
.term-line {{ margin-bottom: 6px; border-bottom: 1px dotted #2d1b4e; padding-bottom: 4px; display: flex; justify-content: space-between; align-items: center; text-transform: uppercase; }}
.term-content {{ flex-grow: 1; }}
.term-time {{ color: #a855f7; font-weight: bold; margin-right: 10px; }}
.term-user {{ color: #00e5ff; font-weight: bold; margin-right: 8px; }}

.sect-header {{ font-size: 1.1em; font-weight: 400; color: #ffffff; border-bottom: 1px solid #1f3b5c; padding-bottom: 5px; margin: 15px 0 10px 0; letter-spacing: 4px; display: flex; align-items: center; }}
.sect-header::before {{ content: ''; display: inline-block; width: 30px; height: 12px; background: #00e5ff; border-radius: 6px 0 0 6px; margin-right: 12px; }}

.ticker-wrap {{ width: 100%; overflow: hidden; background-color: #cc2222; padding: 6px 0; position: fixed; bottom: 0; left: 0; z-index: 999; border-top: 2px solid #ff3333; }}
.ticker {{ display: inline-block; white-space: nowrap; padding-left: 100%; animation: ticker 25s linear infinite; color: #fff; font-size: 1.1em; font-weight: 700; letter-spacing: 3px; }}
@keyframes ticker {{ 0% {{ transform: translate3d(0, 0, 0); }} 100% {{ transform: translate3d(-100%, 0, 0); }} }}

div[data-testid="stButton"] > button {{ border-radius: 20px; border: 1px solid #00e5ff; background: transparent; color: #00e5ff; font-weight: 700; width: 100%; padding: 0px 5px !important; min-height: 38px !important; letter-spacing: 2px; transition: all 0.2s; }}
div[data-testid="stButton"] > button:hover {{ background: #00e5ff; color: #000; box-shadow: 0 0 10px rgba(0, 229, 255, 0.4); }}
</style>
""", unsafe_allow_html=True)

# -------------------------
# WRITE ACTIONS
# -------------------------
def assign_task(task_id: str, widget_key: str) -> None:
    staff = st.session_state.get(widget_key, "Unassigned")
    try:
        supabase.table("tasks").update({"assigned_to": str(staff)}).eq("task_id", str(task_id)).execute()
        clear_fast_cache()
    except Exception as e:
        _log_error(f"assign_task({task_id})", e)
        st.toast(f"Assignment failed for '{staff}'. See admin error log.", icon="🛑")


def complete_task(task_id: str, user: str) -> None:
    st.session_state["hidden_t"].append(str(task_id))
    try:
        supabase.table("tasks").update(
            {"status": "Closed", "closed_by": user, "time_closed": utc_now_iso()}
        ).eq("task_id", str(task_id)).execute()
        clear_fast_cache()
    except Exception as e:
        _safe_remove(st.session_state["hidden_t"], str(task_id))
        _log_error(f"complete_task({task_id})", e)
        st.toast("⚠️ Task could not be closed — restored. Try again.", icon="⚠️")


def complete_oos(oos_id: str, user: str) -> None:
    st.session_state["hidden_o"].append(str(oos_id))
    try:
        supabase.table("oos").update(
            {"status": "Closed", "closed_by": user, "time_closed": utc_now_iso()}
        ).eq("oos_id", str(oos_id)).execute()
        clear_fast_cache()
    except Exception as e:
        _safe_remove(st.session_state["hidden_o"], str(oos_id))
        _log_error(f"complete_oos({oos_id})", e)
        st.toast("⚠️ OOS could not be closed — restored.", icon="⚠️")


def complete_special_order(order_id: str, user: str) -> None:
    st.session_state["hidden_s"].append(str(order_id))
    try:
        supabase.table("special_orders").update(
            {"status": "Closed", "closed_by": user, "time_closed": utc_now_iso()}
        ).eq("order_id", str(order_id)).execute()
        clear_fast_cache()
    except Exception as e:
        _safe_remove(st.session_state["hidden_s"], str(order_id))
        _log_error(f"complete_special_order({order_id})", e)
        st.toast("⚠️ Special order could not be closed — restored.", icon="⚠️")


def complete_expected_order(exp_id: str, user: str) -> None:
    st.session_state["hidden_e"].append(str(exp_id))
    try:
        supabase.table("expected_orders").update(
            {"status": "Closed", "closed_by": user, "time_closed": utc_now_iso()}
        ).eq("exp_id", str(exp_id)).execute()
        clear_fast_cache()
    except Exception as e:
        _safe_remove(st.session_state["hidden_e"], str(exp_id))
        _log_error(f"complete_expected_order({exp_id})", e)
        st.toast("⚠️ Expected order could not be closed — restored.", icon="⚠️")


def undo_action(item_id: str, item_type: str) -> None:
    try:
        if item_type == "task":
            _safe_remove(st.session_state["hidden_t"], item_id)
            supabase.table("tasks").update(
                {"status": "Open", "closed_by": "", "time_closed": None}
            ).eq("task_id", str(item_id)).execute()
        elif item_type == "oos":
            _safe_remove(st.session_state["hidden_o"], item_id)
            supabase.table("oos").update(
                {"status": "Open", "closed_by": "", "time_closed": None}
            ).eq("oos_id", str(item_id)).execute()
        clear_fast_cache()
    except Exception as e:
        _log_error(f"undo_action({item_type}, {item_id})", e)
        st.toast("⚠️ Undo failed — refresh and try again.", icon="⚠️")


def execute_omni_command(cmd: str, user: str, is_quick_key: bool = False) -> None:
    try:
        cmd_l = sanitize_input(cmd, max_len=300, allow_empty=False).lower()
    except ValueError as e:
        st.toast(f"❌ Invalid command: {e}", icon="❌")
        return

    if any(w in cmd_l for w in PRIORITY_URGENT_WORDS):
        pri = "Urgent"
    elif any(w in cmd_l for w in PRIORITY_HIGH_WORDS):
        pri = "High"
    else:
        pri = "Routine"

    zone = "General"
    for z in AISLES:
        if z.lower() in cmd_l:
            zone = z
            break
    if "receiving" in cmd_l or "bale" in cmd_l:
        zone = "Receiving"

    desc = cmd_l.upper()
    try:
        supabase.table("tasks").insert({
            "task_id": gen_id(), "task_detail": desc, "status": "Open",
            "priority": pri, "zone": zone, "assigned_to": "Unassigned",
            "est_mins": 15, "time_submitted": utc_now_iso(), "closed_by": "", "time_closed": None,
        }).execute()
        if not is_quick_key:
            st.toast(f"SYSTEM: Deploying '{desc}'", icon="⚙️")
        clear_fast_cache()
    except Exception as e:
        err_str = str(e)
        if "23505" in err_str or "idx_tasks_unique_open" in err_str:
            st.toast(f"⚠️ '{desc}' is already active on the board.", icon="⚠️")
        else:
            _log_error(f"execute_omni_command('{desc}')", e)
            st.toast("❌ Failed to dispatch command.", icon="❌")


def load_daily_rhythm(grocery_pcs: int, frozen_pcs: int, staff_num: int, cph: float) -> None:
    total_pcs = grocery_pcs + frozen_pcs
    if total_pcs > 0 and cph > 0 and staff_num > 0:
        hrs_math = int(((total_pcs / cph) / staff_num) * 60)
    else:
        hrs_math = 120

    curr_date  = yeg_now()
    curr_day   = curr_date.strftime("%A")
    curr_month = curr_date.month

    ds = [
        {"Task": "Direction Huddle",                  "Priority": "Urgent",  "Zone": "General", "Time": 5},
        {"Task": "Store Walk",                        "Priority": "High",    "Zone": "General", "Time": 30},
        {"Task": "FIFO Audit (Pick 1 Random Aisle)",  "Priority": "Routine", "Zone": "General", "Time": 15},
        {"Task": "Level off displays",                "Priority": "Routine", "Zone": "General", "Time": 10},
    ]

    if curr_day in ("Sunday", "Tuesday", "Thursday"):
        ds.append({"Task": "TGP Order", "Priority": "Urgent", "Zone": "Receiving", "Time": hrs_math})
    else:
        for n in range(1, 9):
            ds.append({"Task": f"Back stock Aisle {n}", "Priority": "Routine", "Zone": f"Aisle {n}", "Time": 45})
        ds.append({"Task": "Back stock Freezer",         "Priority": "Routine", "Zone": "Freezer",  "Time": 45})
        ds.append({"Task": "Check items out of the air", "Priority": "Routine", "Zone": "General",  "Time": 30})

    if curr_day == "Wednesday":
        ds.append({"Task": "PRIMARY AD CHANGEOVER", "Priority": "Urgent", "Zone": "General", "Time": 240})
    if curr_day == "Tuesday":
        ds.append({"Task": "MERCH ENGINE: Verify Dairy Kill Dates",  "Priority": "High", "Zone": "Freezer", "Time": 30})
        ds.append({"Task": "MERCH ENGINE: Verify Bread Expirations", "Priority": "High", "Zone": "Bakery",  "Time": 15})
    if curr_month in WINTER_MONTHS:
        ds.append({"Task": "EDMONTON PROTOCOL: Salt Front Entrance",         "Priority": "High", "Zone": "Outside", "Time": 10})
        ds.append({"Task": "EDMONTON PROTOCOL: Clear Snow from Back Stairs", "Priority": "High", "Zone": "Outside", "Time": 15})

    # Prepare batches
    tasks_to_insert = []
    for d in ds:
        tasks_to_insert.append({
            "task_id": gen_id(), "task_detail": d["Task"].upper(), "status": "Open",
            "priority": d["Priority"], "zone": d["Zone"], "assigned_to": "Unassigned",
            "est_mins": d["Time"], "time_submitted": utc_now_iso(), "closed_by": "", "time_closed": None,
        })

    vendors_to_insert = []
    for v in VENDOR_SCHEDULE.get(curr_day, []):
        vendors_to_insert.append({
            "exp_id": gen_id(), "vendor": v.upper(), "expected_day": curr_day,
            "status": "Pending", "logged_by": "AUTO", "closed_by": "", "time_closed": None,
        })

    # Execute batch inserts
    try:
        if tasks_to_insert:
            supabase.table("tasks").insert(tasks_to_insert).execute()
        if vendors_to_insert:
            supabase.table("expected_orders").insert(vendors_to_insert).execute()
        st.toast("Rhythm Loaded Successfully", icon="📅")
    except Exception as e:
        _log_error("rhythm batch insert", e)
        st.error("⚠️ Failed to load rhythm. Check error logs.")

    clear_fast_cache()


def update_setting(name: str, value) -> bool:
    try:
        supabase.table("settings").delete().eq("setting_name", name).execute()
        supabase.table("settings").insert({"setting_name": name, "setting_value": str(value)}).execute()
        clear_full_cache()
        return True
    except Exception as e:
        _log_error(f"update_setting({name})", e)
        st.toast(f"⚠️ Failed to save setting '{name}'.", icon="⚠️")
        return False

# -------------------------
# SIDEBAR
# -------------------------
if not is_cs_mode and not is_tv_settings_mode and not st.session_state.get("force_tv_settings", False):
    with st.sidebar:
        conn_color = "#00ff00" if (datetime.now().second % 10) < 5 else "#00cc00"
        st.markdown(
            f"<div style='display:flex; align-items:center;'>"
            f"<div style='width:10px;height:10px;border-radius:50%;background-color:{conn_color};"
            f"margin-right:10px;box-shadow:0 0 8px {conn_color};'></div>"
            f"<div style='color:#00e5ff; font-weight:300; letter-spacing:3px; font-size:18px;'>UPLINK ACTIVE</div></div>",
            unsafe_allow_html=True,
        )
        st.markdown("<br>", unsafe_allow_html=True)

        tv_toggle = st.toggle("📺 Local TV Display Mode", value=st.session_state["should_auto_refresh"], key="tv_toggle")
        if tv_toggle != st.session_state["should_auto_refresh"]:
            st.session_state["should_auto_refresh"] = tv_toggle
            st.rerun()

        active_op = st.selectbox("Operator:", PREMIUM_STAFF)

        if st.session_state["write_errors"]:
            with st.expander(f"⚠️ {len(st.session_state['write_errors'])} Write Error(s)", expanded=False):
                for err in reversed(st.session_state["write_errors"]):
                    st.caption(err)
                if st.button("Clear Error Log"):
                    st.session_state["write_errors"] = []
                    st.rerun()

        @st.fragment
        def render_sidebar_tools():
            if not st.session_state["should_auto_refresh"] and not st.session_state.get("show_analytics", False):
                if st.button("🔄 Force Sync Now"):
                    clear_fast_cache()
                    st.rerun()

            with st.form("omni_form", clear_on_submit=True):
                st.markdown("**System Command Line**", unsafe_allow_html=True)
                omni_cmd = st.text_input("Natural Language Entry", placeholder="e.g. urgent spill in aisle 4")
                if st.form_submit_button("Deploy Command") and omni_cmd.strip():
                    execute_omni_command(omni_cmd, active_op)

            c1, c2, c3 = st.columns(3)
            if c1.button("💦 Spill",  use_container_width=True): execute_omni_command("Urgent Spill in General", active_op, True)
            if c2.button("📦 Bale",   use_container_width=True): execute_omni_command("High Priority Make Cardboard Bale", active_op, True)
            if c3.button("🧹 Sweep",  use_container_width=True): execute_omni_command("High Priority Store Safety Sweep", active_op, True)

            with st.expander("➕ Manual Task Override"):
                with st.form("manual_task", clear_on_submit=True):
                    m_task = st.text_input("Task Description")
                    m_pri  = st.selectbox("Priority", ["Routine", "High", "Urgent"])
                    m_zone = st.selectbox("Zone", ["General"] + AISLES)
                    m_time = st.number_input("Est. Mins", min_value=1, value=15, max_value=480)
                    if st.form_submit_button("Deploy Task"):
                        try:
                            task_desc = sanitize_input(m_task, max_len=300)
                            est_mins = validate_integer(m_time, min_val=1, max_val=480)
                            supabase.table("tasks").insert({
                                "task_id": gen_id(), "task_detail": task_desc.upper(),
                                "status": "Open", "priority": m_pri, "zone": m_zone,
                                "assigned_to": "Unassigned", "est_mins": est_mins,
                                "time_submitted": utc_now_iso(), "closed_by": "", "time_closed": None,
                            }).execute()
                            clear_fast_cache()
                            st.toast("Task deployed.", icon="✅")
                        except ValueError as e:
                            st.error(f"❌ {e}")
                        except Exception as e:
                            err_str = str(e)
                            if "23505" in err_str or "idx_tasks_unique_open" in err_str:
                                st.warning(f"Task is already active.")
                            else:
                                _log_error("manual task insert", e)
                                st.error("Failed to save task.")

            st.divider()

            with st.expander("📝 Shift Notes & Ticker"):
                new_note = st.text_area("Pass the baton:", value=shift_notes, height=100)
                is_crit  = st.checkbox("Mark as Critical Alert", value=is_critical_alert)
                if st.button("Save Handover Notes"):
                    update_setting("Shift_Notes",    html.escape(new_note.strip()))
                    update_setting("Critical_Alert", "1" if is_crit else "0")
                    st.toast("Notes Updated", icon="✅")
                    st.rerun()

                st.markdown("<hr style='margin: 10px 0; border-color: #1f3b5c;'>", unsafe_allow_html=True)
                with st.form("ticker_form", clear_on_submit=True):
                    t_msg = st.text_input("Broadcast Live Ticker Message")
                    if st.form_submit_button("Send to Ticker") and t_msg.strip():
                        try:
                            supabase.table("ticker").insert(
                                {"msg_id": gen_id(), "message": html.escape(t_msg.strip().upper())}
                            ).execute()
                            clear_full_cache()
                            st.rerun()
                        except Exception as e:
                            _log_error("ticker insert", e)
                            st.toast("⚠️ Ticker message failed to send.", icon="⚠️")

            with st.expander("👥 Shift Roster Settings"):
                selected_active = st.multiselect("Active Today:", master_staff, default=active_staff)
                if st.button("Update Roster"):
                    try:
                        supabase.table("staff").update({"active": 0}).neq("name", "Unassigned").execute()
                        if selected_active:
                            supabase.table("staff").update({"active": 1}).in_("name", selected_active).execute()
                        clear_full_cache()
                        st.rerun()
                    except Exception as e:
                        _log_error("roster update", e)
                        st.toast("⚠️ Roster update failed.", icon="⚠️")

                st.markdown("<hr style='margin: 10px 0; border-color: #1f3b5c;'>", unsafe_allow_html=True)
                new_staff = st.text_input("Add New Team Member")
                if st.button("Add to Database") and new_staff.strip():
                    try:
                        supabase.table("staff").insert({"name": new_staff.strip().title(), "active": 1}).execute()
                        clear_full_cache()
                        st.rerun()
                    except Exception as e:
                        _log_error("staff insert", e)
                        st.toast("⚠️ Could not add team member.", icon="⚠️")

            with st.form("load_form"):
                c_time1, c_time2 = st.columns(2)
                in_arr = c_time1.time_input("Start of Order", value=datetime.strptime(start_time_str, "%H:%M").time())
                in_end = c_time2.time_input("End of Order",   value=datetime.strptime(end_time_str,   "%H:%M").time())
                c1, c2 = st.columns(2)
                in_g = c1.number_input("Groc Pcs",    min_value=0, value=g_pcs)
                in_f = c2.number_input("Froz Pcs",    min_value=0, value=f_pcs)
                in_s = st.number_input("Active Staff", min_value=1, value=staff_count)
                if st.form_submit_button("Calculate Labor"):
                    try:
                        supabase.table("counts").update({
                            "grocery": in_g, "frozen": in_f, "staff": in_s, "last_update": utc_now_iso()
                        }).eq("id", 1).execute()
                        update_setting("Start_Time", in_arr.strftime("%H:%M"))
                        update_setting("End_Time",   in_end.strftime("%H:%M"))
                        st.rerun()
                    except Exception as e:
                        _log_error("counts update", e)
                        st.toast("⚠️ Failed to save labor data.", icon="⚠️")

            with st.form("oos_form", clear_on_submit=True):
                o_z = st.selectbox("Log Shelf Holes", AISLES[:8] + ["Freezer", "Bakery"])
                c1, c2 = st.columns(2)
                o_c = c1.number_input("Qty Holes", min_value=1, value=1)
                o_n = c2.text_input("Notes")
                if st.form_submit_button("Log OOS"):
                    try:
                        supabase.table("oos").insert({
                            "oos_id": gen_id(), "zone": o_z, "hole_count": o_c,
                            "notes": o_n.strip().upper(), "status": "Open",
                            "logged_by": active_op, "time_logged": utc_now_iso(),
                            "closed_by": "", "time_closed": None,
                        }).execute()
                        clear_fast_cache()
                        st.toast("OOS logged.", icon="📋")
                    except Exception as e:
                        _log_error("oos insert", e)
                        st.toast("⚠️ OOS log failed.", icon="⚠️")

            with st.expander("🛠️ Advanced Loggers"):
                with st.form("vendor_form", clear_on_submit=True):
                    e_ven = st.text_input("Log Expected Vendor")
                    if st.form_submit_button("Log") and e_ven.strip():
                        try:
                            supabase.table("expected_orders").insert({
                                "exp_id": gen_id(), "vendor": e_ven.strip().upper(),
                                "expected_day": yeg_now().strftime("%A"), "status": "Pending",
                                "logged_by": active_op, "closed_by": "", "time_closed": None,
                            }).execute()
                            clear_fast_cache()
                            st.toast("Vendor logged.", icon="🚚")
                        except Exception as e:
                            _log_error("vendor insert", e)
                            st.toast("⚠️ Vendor log failed.", icon="⚠️")

                new_prog = st.slider("Seasonal Changeover %", 0, 100, seasonal_progress, step=10)
                if st.button("Update Tracker"):
                    update_setting("Seasonal_Progress", new_prog)
                    st.rerun()

            # --- ADMIN CONSOLE ---
            st.divider()
            st.markdown("### 🔒 SYSTEM ADMIN")

            if not admin_pin_hash:
                st.warning("⚠️ Admin PIN not configured. Cannot access admin panel.")
                st.info("Set Admin_PIN in database settings first.")
            else:
                admin_pass = st.text_input("Admin PIN", type="password")
                pin_entered = admin_pass != ""
                pin_correct = pin_entered and verify_pin(admin_pass, admin_pin_hash)
                admin_open = pin_correct

                if not pin_entered:
                    st.caption("Enter PIN to unlock admin panel")
                elif not pin_correct:
                    st.error("❌ Incorrect PIN")
                elif admin_open:
                    st.success("✅ Admin Unlocked")

                if st.button(
                    "⬅️ Close Analytics" if st.session_state.get("show_analytics", False) else "📈 Launch Analytics",
                    type="primary", use_container_width=True,
                ):
                    st.session_state["show_analytics"] = not st.session_state.get("show_analytics", False)
                    st.rerun()

                with st.expander("⚙️ System Config"):
                    new_scale = st.slider("TV UI Scale (%)", 80, 150, font_scale, step=5)
                    if st.button("Save Scale"):
                        update_setting("TV_Scale", new_scale)
                        st.rerun()

                    new_cph = st.number_input("Cases Per Hour Target", min_value=10.0, max_value=200.0,
                                              value=cases_per_hour, step=5.0)
                    if st.button("Save CPH"):
                        update_setting("Cases_Per_Hour", new_cph)
                        st.rerun()

                    st.divider()
                    st.caption("Database Health (Rows)")
                    try:
                        t_count = supabase.table("tasks").select("*", count="exact").execute().count
                        o_count = supabase.table("oos").select("*", count="exact").execute().count
                        st.code(f"Tasks Table: {t_count}\nOOS Table:   {o_count}")
                    except Exception:
                        st.code("DB Read Error")

                    if st.button("🚨 HARD REBOOT SERVER"):
                        clear_full_cache()
                        st.toast("Server Cache Flushed", icon="🔥")
                        st.rerun()

                with st.expander("🗄️ Data Archival & Pruning"):
                    cutoff_30 = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
                    try:
                        old_tasks = to_df(supabase.table("tasks").select("*")
                                         .in_("status", ["Closed", "Archived"]).lt("time_closed", cutoff_30).execute().data)
                        old_oos   = to_df(supabase.table("oos").select("*")
                                         .in_("status", ["Closed", "Archived"]).lt("time_closed", cutoff_30).execute().data)

                        if old_tasks.empty and old_oos.empty:
                            st.info("No records older than 30 days found.")
                        else:
                            st.success(f"Found {len(old_tasks)} old tasks, {len(old_oos)} OOS records.")
                            arch_buffer = io.BytesIO()
                            with pd.ExcelWriter(arch_buffer, engine="openpyxl") as writer:
                                if not old_tasks.empty: old_tasks.to_excel(writer, sheet_name="Archived_Tasks", index=False)
                                if not old_oos.empty:   old_oos.to_excel(writer,   sheet_name="Archived_OOS",   index=False)
                            st.download_button(
                                "📥 1. Download Backup (Excel)",
                                data=arch_buffer.getvalue(),
                                file_name=f"TGP_Archive_{yeg_now().strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                            )
                            st.warning("Download backup before purging!")
                            if st.button("🗑️ 2. Purge Database", type="primary", use_container_width=True):
                                try:
                                    supabase.table("tasks").delete().in_("status", ["Closed", "Archived"]).lt("time_closed", cutoff_30).execute()
                                    supabase.table("oos").delete().in_("status", ["Closed", "Archived"]).lt("time_closed", cutoff_30).execute()
                                    clear_full_cache()
                                    st.rerun()
                                except Exception as e:
                                    _log_error("purge database", e)
                                    st.error("Purge failed. Check error log.")
                    except Exception as e:
                        _log_error("archival data load", e)
                        st.error("Failed to load archival data.")

                with st.expander("🚨 Full Board Reset"):
                    st.warning("This archives ALL open items and clears the ticker. Cannot be undone.")
                    eod_confirm = st.checkbox("I understand — proceed with EOD Sweep")
                    if eod_confirm and st.button("Execute EOD Sweep", type="primary"):
                        t_now = utc_now_iso()
                        try:
                            supabase.table("tasks").update({"status": "Archived", "closed_by": "AUTO", "time_closed": t_now}).eq("status", "Open").execute()
                            supabase.table("oos").update({"status": "Archived", "closed_by": "AUTO", "time_closed": t_now}).eq("status", "Open").execute()
                            supabase.table("special_orders").update({"status": "Archived", "closed_by": "AUTO", "time_closed": t_now}).eq("status", "Open").execute()
                            supabase.table("expected_orders").update({"status": "Archived", "closed_by": "AUTO", "time_closed": t_now}).eq("status", "Pending").execute()
                            supabase.table("tasks").update({"status": "Archived"}).eq("status", "Closed").execute()
                            supabase.table("oos").update({"status": "Archived"}).eq("status", "Closed").execute()
                            supabase.table("special_orders").update({"status": "Archived"}).eq("status", "Closed").execute()
                            supabase.table("expected_orders").update({"status": "Archived"}).eq("status", "Closed").execute()
                            try:
                                supabase.table("ticker").delete().neq("message", "xyz_impossible_match").execute()
                            except Exception:
                                pass
                            for k in ["hidden_t", "hidden_o", "hidden_s", "hidden_e"]:
                                st.session_state[k] = []
                            clear_full_cache()
                            st.rerun()
                        except Exception as e:
                            _log_error("EOD sweep", e)
                            st.error("EOD sweep failed partway. Refresh and check the board.")

        render_sidebar_tools()

# -------------------------
# CUSTOMER SERVICE PORTAL
# -------------------------
def render_cs_desk() -> None:
    st.markdown("<div class='header-bar'><div class='header-title'>CUSTOMER SERVICE // SPECIAL ORDERS</div></div>", unsafe_allow_html=True)
    st.write("Log items requested by customers here. They will instantly appear on the Operations board on the floor.")

    with st.form("cs_order_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        c_name    = c1.text_input("Customer Name (Required)")
        c_contact = c2.text_input("Contact Info (Phone/Email)")
        i1, i2   = st.columns([3, 1])
        c_item    = i1.text_input("Item Description (Required)")
        c_loc     = i2.selectbox("Order Location", ORDER_LOCATIONS)
        cs_rep    = st.text_input("Your Name (CS Rep)")
        submit    = st.form_submit_button("SEND TO FLOOR 🚀", use_container_width=True)

        if submit:
            try:
                c_name_val = sanitize_input(c_name, max_len=100)
                c_item_val = sanitize_input(c_item, max_len=200)
                cs_rep_val = sanitize_input(cs_rep, max_len=100)
                c_contact_val = sanitize_input(c_contact, max_len=100, allow_empty=True)

                supabase.table("special_orders").insert({
                    "order_id":    gen_id(),
                    "customer":    c_name_val.upper(),
                    "item":        c_item_val.upper(),
                    "contact":     c_contact_val.upper(),
                    "location":    c_loc,
                    "status":      "Open",
                    "logged_by":   cs_rep_val.upper(),
                    "time_logged": utc_now_iso(),
                    "closed_by":   "",
                    "time_closed": None,
                }).execute()
                st.success(f"✅ Order has been sent to the floor!")
                clear_fast_cache()
            except ValueError as e:
                st.error(f"❌ {e}")
            except Exception as e:
                _log_error("cs_order insert", e)
                st.error("Failed to send order. Check connection.")

# -------------------------
# TV SETTINGS MENU
# -------------------------
def render_tv_settings() -> None:
    st.markdown("<div class='header-bar'><div class='header-title'>TV MODE // DISPLAY SETTINGS</div></div>", unsafe_allow_html=True)
    st.info("⏸️ Auto-scroll is currently paused. Adjust the zoom level below.")
    new_scale = st.slider("TV UI Scale (%)", 80, 200, font_scale, step=5)
    st.markdown("<br>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    if c1.button("✅ Save & Return to Board", type="primary", use_container_width=True):
        update_setting("TV_Scale", new_scale)
        st.session_state["force_tv_settings"] = False
        st.rerun()
    if c2.button("❌ Cancel", use_container_width=True):
        st.session_state["force_tv_settings"] = False
        st.rerun()

# -------------------------
# ANALYTICS
# -------------------------
def render_analytics() -> None:
    st.markdown("## 📊 SYSTEM ANALYTICS")
    c1, _ = st.columns([0.3, 0.7])
    target_date = c1.date_input("Time Machine: Select Date", value=yeg_now().date())
    history = load_historical_data(target_date.strftime("%Y-%m-%d"))
    t_df = history["tasks"]
    o_df = history["oos"]

    if t_df.empty and o_df.empty:
        st.warning(f"No closed data found for {target_date.strftime('%B %d, %Y')}.")
        return

    st.divider()
    m1, m2, m3 = st.columns(3)
    m1.metric("Tasks Completed", len(t_df))
    avg_mins = round(t_df["actual_mins"].dropna().mean(), 1) if not t_df.empty and "actual_mins" in t_df.columns else "N/A"
    m2.metric("Avg Time to Close (Mins)", avg_mins)
    m3.metric("Total Shelf Holes Logged", int(o_df["hole_count"].sum()) if "hole_count" in o_df.columns and not o_df.empty else 0)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        if not t_df.empty:
            t_df.drop(columns=["time_submitted", "time_closed"], errors="ignore").to_excel(writer, sheet_name="Closed_Tasks", index=False)
        if not o_df.empty:
            o_df.to_excel(writer, sheet_name="Closed_OOS", index=False)
    st.download_button(
        "📥 Export Report to Excel",
        data=buffer.getvalue(),
        file_name=f"TGP_Report_{target_date.strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    if not t_df.empty:
        st.subheader("Staff Throughput")
        if "closed_by" in t_df.columns:
            staff_counts = (t_df.groupby("closed_by").size()
                            .reset_index(name="tasks_closed")
                            .sort_values("tasks_closed", ascending=False))
            st.bar_chart(staff_counts, x="closed_by", y="tasks_closed", color="#00e5ff")

        st.subheader("Completion Timeline")
        if "time_closed" in t_df.columns:
            timeline = t_df.dropna(subset=["time_closed"]).copy()
            timeline["hour"] = timeline["time_closed"].dt.hour
            hourly = timeline.groupby("hour").size().reset_index(name="tasks_closed")
            if not hourly.empty:
                st.bar_chart(hourly, x="hour", y="tasks_closed", color="#a855f7")

# -------------------------
# MAIN BOARD
# -------------------------
def render_main_board(fast_snap: dict, is_tv: bool) -> None:
    t_df   = fast_snap["tasks"].copy()
    oos_df = fast_snap["oos"].copy()
    s_df   = fast_snap["orders"].copy()
    e_df   = fast_snap["expected"].copy()
    a_df   = fast_snap["audit"].copy() if isinstance(fast_snap["audit"], pd.DataFrame) else pd.DataFrame()

    _filter_map = [
        ("task_id",  "hidden_t"),
        ("oos_id",   "hidden_o"),
        ("order_id", "hidden_s"),
        ("exp_id",   "hidden_e"),
    ]
    dfs = {"task_id": t_df, "oos_id": oos_df, "order_id": s_df, "exp_id": e_df}
    for col, hidden_key in _filter_map:
        hidden = st.session_state.get(hidden_key, [])
        df     = dfs[col]
        if hidden and not df.empty and col in df.columns:
            dfs[col] = df[~df[col].astype(str).isin(hidden)]
    t_df, oos_df, s_df, e_df = dfs["task_id"], dfs["oos_id"], dfs["order_id"], dfs["exp_id"]

    curr_now = yeg_now()
    now_utc  = datetime.now(timezone.utc)

    safe_notes = html.escape(shift_notes)
    if is_critical_alert and shift_notes:
        st.markdown(f"<div class='alert-banner'>🚨 CRITICAL ALERT: {safe_notes}</div>", unsafe_allow_html=True)
    elif shift_notes:
        st.markdown(f"<div class='shift-note'>📌 <strong>HANDOVER NOTES:</strong> {safe_notes}</div>", unsafe_allow_html=True)

    if 0 < seasonal_progress < 100:
        st.progress(seasonal_progress / 100.0, text=f"Seasonal Changeover Progress: {seasonal_progress}%")

    st.markdown(
        f"<div class='header-bar'>"
        f"<div class='header-title'>TGP CENTRE STORE // {curr_now.strftime('%A')}</div>"
        f"<div class='header-time'>{curr_now.strftime('%b %d, %Y')} | {curr_now.strftime('%H:%M')}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    g, f      = g_pcs, f_pcs
    total_pcs = g + f
    t_mins    = pd.to_numeric(t_df["est_mins"], errors="coerce").fillna(15).sum() if not t_df.empty else 0
    f_hrs     = (total_pcs / cases_per_hour) if cases_per_hour > 0 else 0
    total_hrs = (f_hrs + (t_mins / 60.0)) / staff_count

    arr_time_obj = datetime.strptime(start_time_str, "%H:%M").time()
    end_time_obj = datetime.strptime(end_time_str,   "%H:%M").time()
    anchor_time  = curr_now.replace(hour=arr_time_obj.hour, minute=arr_time_obj.minute, second=0, microsecond=0)
    end_anchor   = curr_now.replace(hour=end_time_obj.hour, minute=end_time_obj.minute, second=0, microsecond=0)
    if end_anchor <= anchor_time:
        end_anchor += timedelta(days=1)

    eta = (anchor_time + timedelta(hours=total_hrs)).strftime("%H:%M") if (total_pcs > 0 or t_mins > 0) else "N/A"

    available_hrs      = (end_anchor - anchor_time).total_seconds() / 3600.0
    task_hrs_per_staff = (t_mins / 60.0) / staff_count if staff_count > 0 else 0
    piece_hrs_avail    = available_hrs - task_hrs_per_staff

    if total_pcs == 0:
        req_cph_display, cph_css = "0/h", ""
    elif piece_hrs_avail > 0:
        req_cph         = (total_pcs / staff_count) / piece_hrs_avail
        req_cph_display = f"{int(req_cph)}/h"
        cph_css         = "urgent" if req_cph > (cases_per_hour + 15) else "amber" if req_cph > cases_per_hour else ""
    else:
        req_cph_display, cph_css = "MAX", "urgent"

    st.markdown(
        f"<div class='kpi-container'>"
        f"<div class='kpi-box'><div class='kpi-label'>Grocery</div><div class='kpi-value'>{g}</div></div>"
        f"<div class='kpi-box'><div class='kpi-label'>Frozen</div><div class='kpi-value'>{f}</div></div>"
        f"<div class='kpi-box'><div class='kpi-label'>Staff</div><div class='kpi-value'>{staff_count}</div></div>"
        f"<div class='kpi-box'><div class='kpi-label'>Tasks</div><div class='kpi-value'>{int(t_mins)}m</div></div>"
        f"<div class='kpi-box'><div class='kpi-label'>Target End</div><div class='kpi-value'>{end_time_str}</div></div>"
        f"<div class='kpi-box'><div class='kpi-label'>Est. Finish</div><div class='kpi-value'>{eta}</div></div>"
        f"<div class='kpi-box {cph_css}'><div class='kpi-label'>Req. Speed</div><div class='kpi-value'>{req_cph_display}</div></div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    L, R = st.columns([0.65, 0.35], gap="small")

    with L:
        st.markdown("<div class='sect-header'>Active Directives</div>", unsafe_allow_html=True)
        surge_active = (total_hrs > 7.5) or (cph_css == "urgent")
        if surge_active:
            st.error("⚠️ SURGE MODE: Routine tasks suppressed. High operational velocity required.")

        if t_df.empty:
            st.success("All tasks complete!")
        else:
            t_df["time_submitted_dt"] = pd.to_datetime(t_df["time_submitted"], utc=True, errors="coerce")
            t_df["age_mins"]          = (now_utc - t_df["time_submitted_dt"]).dt.total_seconds() / 60.0
            t_df["age_mins"]          = t_df["age_mins"].fillna(0)

            if surge_active:
                t_df = t_df[(t_df["priority"] != "Routine") | (t_df["age_mins"] > 90)]

            t_df["p_rank"] = t_df["priority"].map({"Urgent": 1, "High": 2, "Routine": 3}).fillna(3)
            t_df = t_df.sort_values(["p_rank", "age_mins"], ascending=[True, False])

            for _, r in t_df.iterrows():
                age     = r["age_mins"]
                age_str = f"T+{int(age)}m" if age > 5 else "NEW"
                p_class = ("data-urgent" if (r["priority"] == "Urgent" or age > 90)
                           else "data-high" if (r["priority"] == "High" or age > 45)
                           else "")

                card_html = (
                    f"<div class='data-card {p_class}'>"
                    f"<div><span class='card-zone'>[{html.escape(str(r['zone']))}]</span>"
                    f" {html.escape(str(r['task_detail']))}</div>"
                    f"<div class='card-meta'><strong>{html.escape(str(r['assigned_to']))}</strong>"
                    f"<br>{age_str}</div>"
                    f"</div>"
                )

                if is_tv:
                    st.markdown(card_html, unsafe_allow_html=True)
                else:
                    c1, c2, c3 = st.columns([0.65, 0.20, 0.15], gap="small")
                    c1.markdown(card_html, unsafe_allow_html=True)
                    base_opts = []
                    for x in ["Unassigned", "ALL STAFF"] + active_staff + [str(r["assigned_to"])]:
                        if x not in base_opts:
                            base_opts.append(x)
                    assigned_val = str(r["assigned_to"])
                    sel_index    = base_opts.index(assigned_val) if assigned_val in base_opts else 0
                    c2.selectbox(
                        "Assign", base_opts,
                        index=sel_index,
                        key=f"sel_{r['task_id']}",
                        label_visibility="collapsed",
                        on_change=assign_task,
                        args=(r["task_id"], f"sel_{r['task_id']}"),
                    )
                    c3.button("DONE", key=f"dn_{r['task_id']}", on_click=complete_task, args=(r["task_id"], active_op))

        st.markdown("<div class='sect-header' style='margin-top:20px;'>Live Audit Terminal (Failsafe)</div>", unsafe_allow_html=True)
        if a_df.empty:
            st.markdown("<div class='terminal-box'><div class='term-line'>AWAITING SYSTEM DATA...</div></div>", unsafe_allow_html=True)
        else:
            term_html = "<div class='terminal-box'>"
            for _, r in a_df.iterrows():
                try:
                    dt_str = datetime.fromisoformat(str(r["time"])).astimezone(LOCAL_TZ).strftime("%H:%M:%S")
                except Exception:
                    dt_str = "00:00:00"
                term_html += (
                    f"<div class='term-line'><div class='term-content'>"
                    f"<span class='term-time'>[{dt_str}]</span>"
                    f"<span class='term-user'>{html.escape(str(r['user']))}:</span> "
                    f"{html.escape(str(r['event']))}"
                    f"</div></div>"
                )
            term_html += "</div>"
            st.markdown(term_html, unsafe_allow_html=True)

            if not is_tv:
                st.caption("Failsafe: Reopen recent items")
                ucols = st.columns(min(len(a_df), 6))
                for i, (_, r) in enumerate(a_df.head(6).iterrows()):
                    trunc_name = str(r["event"])[:10] + ".."
                    ucols[i].button(f"↩️ {trunc_name}", key=f"undo_{r['id']}", help="Reopen",
                                    on_click=undo_action, args=(r["id"], r["type"]))

    with R:
        st.markdown("<div class='sect-header'>Inventory Flags (OOS)</div>", unsafe_allow_html=True)
        if oos_df.empty:
            st.caption("No inventory anomalies reported.")
        for _, r in oos_df.iterrows():
            oos_html = (
                f"<div class='data-card data-urgent'>"
                f"<div><span class='card-zone'>{html.escape(str(r['zone']))}</span>:"
                f" {r['hole_count']} HOLES</div>"
                f"<div class='card-meta'>{html.escape(str(r.get('notes', '')))}</div>"
                f"</div>"
            )
            if is_tv:
                st.markdown(oos_html, unsafe_allow_html=True)
            else:
                c1, c2 = st.columns([0.80, 0.20], gap="small")
                c1.markdown(oos_html, unsafe_allow_html=True)
                c2.button("CLR", key=f"o_{r['oos_id']}", on_click=complete_oos, args=(r["oos_id"], active_op))

        st.markdown("<div class='sect-header'>Incoming Customer Orders</div>", unsafe_allow_html=True)
        if s_df.empty:
            st.caption("No pending customer orders.")
        for _, r in s_df.iterrows():
            c1, c2 = st.columns([0.80, 0.20], gap="small")
            c1.markdown(
                f"<div class='data-card' style='border-left-color:#a855f7;'>"
                f"<div><span class='card-zone'>L:{html.escape(str(r['location']))}</span>"
                f" {html.escape(str(r['item']))}</div>"
                f"<div class='card-meta'>{html.escape(str(r.get('customer', '')))}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
            if not is_tv:
                c2.button("DONE", key=f"s_{r['order_id']}", on_click=complete_special_order, args=(r["order_id"], active_op))

        st.markdown("<div class='sect-header'>Vendor Deliveries</div>", unsafe_allow_html=True)
        if e_df.empty:
            st.caption("No pending vendor deliveries.")
        for _, r in e_df.iterrows():
            c1, c2 = st.columns([0.80, 0.20], gap="small")
            c1.markdown(
                f"<div class='data-card' style='border-left-color:#ffaa00;'>"
                f"🚚 <span class='card-zone'>{html.escape(str(r['vendor']))}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
            if not is_tv:
                c2.button("RCV", key=f"e_{r['exp_id']}", on_click=complete_expected_order, args=(r["exp_id"], active_op))

        st.divider()
        if not is_tv:
            if st.button("🚀 Load Daily Rhythm", use_container_width=True):
                load_daily_rhythm(g, f, staff_count, cases_per_hour)
                st.rerun()

    # Bottom ticker
    tk_df = slow_data["ticker"]
    if not tk_df.empty:
        live_tk = tk_df.dropna(subset=["message"])
        if not live_tk.empty:
            m_str = " &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; ".join(live_tk["message"].tolist())
            st.markdown(
                f"<div class='ticker-wrap'><div class='ticker'>"
                f"📢 {m_str} &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; "
                f"</div></div>",
                unsafe_allow_html=True,
            )

# -------------------------
# TV AUTO-SCROLL
# -------------------------
if is_tv_url_mode and not is_tv_settings_mode and not is_cs_mode and not st.session_state.get("force_tv_settings", False):
    st.markdown("""
    <script>
    const scrollApp = () => {
        let dir = 1;
        setInterval(() => {
            window.scrollBy(0, dir * 1);
            if ((window.innerHeight + window.scrollY) >= document.body.offsetHeight - 50) dir = -1;
            if (window.scrollY === 0) dir = 1;
        }, 50);
    };
    if (window.top === window.self) scrollApp();
    </script>
    """, unsafe_allow_html=True)

# -------------------------
# FRAGMENTS (Defined globally for stable IDs)
# -------------------------
@st.fragment(run_every=4)
def tv_loop():
    render_main_board(load_fast_data(), is_tv=True)

@st.fragment(run_every=10)
def interactive_loop():
    render_main_board(load_fast_data(), is_tv=False)

# -------------------------
# ENTRYPOINT
# -------------------------
if is_cs_mode:
    render_cs_desk()
elif is_tv_settings_mode or st.session_state.get("force_tv_settings", False):
    render_tv_settings()
elif st.session_state.get("show_analytics", False):
    render_analytics()
else:
    if st.session_state["should_auto_refresh"]:
        tv_loop()

        if is_tv_url_mode:
            st.markdown("<hr style='border-color: #1f3b5c; margin-top: 50px;'>", unsafe_allow_html=True)
            _, c2, _ = st.columns([0.3, 0.4, 0.3])
            with c2:
                if st.button("⚙️ ADJUST TV SCALE ⚙️", use_container_width=True):
                    st.session_state["force_tv_settings"] = True
                    st.rerun()
    else:
        interactive_loop()
