import streamlit as st
import pandas as pd
import uuid
import hashlib
import html
import io
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# -------------------------
# TIMEZONE & HELPERS (EDMONTON LOCK)
# -------------------------
try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Edmonton")
except ImportError:
    import pytz
    LOCAL_TZ = pytz.timezone("America/Edmonton")

def yeg_now():
    return datetime.now(LOCAL_TZ)

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def gen_id():
    return uuid.uuid4().hex

def hash_pin(pin):
    return hashlib.sha256(str(pin).encode()).hexdigest()

# -------------------------
# CONFIG & OPTIMISTIC UI MEMORY
# -------------------------
st.set_page_config(page_title="TGP Operations", layout="wide", initial_sidebar_state="expanded")

# Hidden Memory for Optimistic UI (Zero Latency)
for key in ["hidden_t", "hidden_o", "hidden_s", "hidden_e"]:
    if key not in st.session_state: st.session_state[key] = []
    if len(st.session_state[key]) > 100: st.session_state[key] = st.session_state[key][-50:]

font_scale = st.session_state.get("ui_font_scale", 100)

st.markdown(f"""
<style>
footer {{ visibility: hidden; }}
#MainMenu {{ visibility: hidden; }}

/* BASE TYPOGRAPHY & SCALING */
html, body, .stApp {{ font-size: {font_scale}%; background-color: #000000; color: #88ccff; font-family: 'Arial Narrow', 'Arial', sans-serif; overflow-x: hidden; text-transform: uppercase; }}

/* HEADER TOGGLE FIX */
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

/* HEADER BAR */
.header-bar {{ display: flex; align-items: flex-end; justify-content: space-between; border-bottom: 4px solid #ffaa00; margin-bottom: 15px; padding-bottom: 8px; padding-top: 10px; position: relative; }}
.header-bar::before {{ content: ''; position: absolute; left: 0; bottom: -4px; width: 60px; height: 20px; background: #ffaa00; border-radius: 10px 0 0 10px; }}
.header-title {{ font-size: 2.2em; font-weight: 300; color: #eef5ff; letter-spacing: 5px; margin: 0 0 0 75px; line-height: 0.9; }}
.header-time {{ color: #88ccff; font-size: 1.3em; font-weight: 400; margin: 0; letter-spacing: 2px; }}

/* BULLETPROOF TV SETTINGS BUTTON */
.secret-tv-btn {{ position: absolute; left: 0; bottom: -4px; width: 60px; height: 40px; z-index: 999999; cursor: pointer; display: flex; align-items: center; justify-content: center; text-decoration: none; background: transparent; border-radius: 10px 0 0 10px; }}
.secret-tv-btn::after {{ content: '⚙️'; font-size: 20px; opacity: 0.15; transition: opacity 0.2s; }}
.secret-tv-btn:hover {{ background: rgba(255, 255, 255, 0.2); }}
.secret-tv-btn:hover::after {{ opacity: 1; }}

/* SHIFT NOTES & ALERTS */
.alert-banner {{ background: #ff3333; color: #ffffff; padding: 10px 20px; font-weight: bold; border-radius: 5px; margin-bottom: 15px; border-left: 10px solid #990000; letter-spacing: 2px; }}
.shift-note {{ background: rgba(31, 59, 92, 0.4); border-left: 4px solid #00e5ff; padding: 10px 15px; margin-bottom: 15px; color: #eef5ff; font-size: 1.1em; }}

/* KPI GRID */
.kpi-container {{ display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: 10px; margin-bottom: 20px; width: 100%; }}
.kpi-box {{ background: rgba(11, 26, 46, 0.6); border-right: 5px solid #00e5ff; padding: 10px 15px; border-radius: 15px 0 0 15px; display: flex; flex-direction: column; justify-content: center; }}
.kpi-box.urgent {{ border-right-color: #ff3333; background: rgba(42, 10, 10, 0.6); }}
.kpi-box.amber {{ border-right-color: #ffaa00; background: rgba(42, 31, 10, 0.6); }}
.kpi-label {{ font-size: 0.8em; font-weight: 700; color: #6699cc; letter-spacing: 2px; margin-bottom: 2px; white-space: nowrap; }}
.kpi-value {{ font-size: 1.6em; font-weight: 300; color: #ffffff; white-space: nowrap; line-height: 1; }}

/* DATA CARDS */
.data-card {{ background: rgba(11, 26, 46, 0.5); border-left: 6px solid #00e5ff; padding: 12px 15px; margin-bottom: 8px; border-radius: 0 20px 20px 0; font-size: 1em; line-height: 1.4; display: flex; justify-content: space-between; align-items: center; letter-spacing: 1px; }}
.data-urgent {{ border-left-color: #ff3333; background: rgba(42, 10, 10, 0.5); color: #ffcccc; }}
.data-high {{ border-left-color: #ffaa00; background: rgba(42, 31, 10, 0.5); color: #ffebcc; }}
.card-zone {{ font-weight: 700; color: inherit; letter-spacing: 2px; margin-right: 8px; font-size: 0.9em; opacity: 0.8;}}
.card-meta {{ font-size: 0.75em; text-align: right; color: #6699cc; letter-spacing: 1px; }}
.card-meta strong {{ color: #00e5ff; font-size: 1.1em; }}

/* TERMINAL / LOGS */
.terminal-box {{ background: rgba(10, 5, 20, 0.8); border-left: 3px solid #a855f7; border-radius: 0 10px 10px 0; padding: 12px; font-family: 'Courier New', monospace; font-size: 0.85em; color: #9ca3af; margin-top: 15px; text-transform: none; }}
.term-line {{ margin-bottom: 6px; border-bottom: 1px dotted #2d1b4e; padding-bottom: 4px; display: flex; justify-content: space-between; align-items: center; text-transform: uppercase; }}
.term-content {{ flex-grow: 1; }}
.term-time {{ color: #a855f7; font-weight: bold; margin-right: 10px; }}
.term-user {{ color: #00e5ff; font-weight: bold; margin-right: 8px; }}
.undo-btn {{ background: transparent; color: #ffaa00; border: 1px solid #ffaa00; border-radius: 10px; font-size: 0.8em; padding: 2px 8px; cursor: pointer; }}

/* SECTION HEADERS */
.sect-header {{ font-size: 1.1em; font-weight: 400; color: #ffffff; border-bottom: 1px solid #1f3b5c; padding-bottom: 5px; margin: 15px 0 10px 0; letter-spacing: 4px; display: flex; align-items: center; }}
.sect-header::before {{ content: ''; display: inline-block; width: 30px; height: 12px; background: #00e5ff; border-radius: 6px 0 0 6px; margin-right: 12px; }}

/* TICKER */
.ticker-wrap {{ width: 100%; overflow: hidden; background-color: #cc2222; padding: 6px 0; position: fixed; bottom: 0; left: 0; z-index: 999; border-top: 2px solid #ff3333; }}
.ticker {{ display: inline-block; white-space: nowrap; padding-left: 100%; animation: ticker 25s linear infinite; color: #fff; font-size: 1.1em; font-weight: 700; letter-spacing: 3px; }}
@keyframes ticker {{ 0% {{ transform: translate3d(0, 0, 0); }} 100% {{ transform: translate3d(-100%, 0, 0); }} }}

/* BUTTONS */
div[data-testid="stButton"] > button {{ border-radius: 20px; border: 1px solid #00e5ff; background: transparent; color: #00e5ff; font-weight: 700; width: 100%; padding: 0px 5px !important; min-height: 38px !important; letter-spacing: 2px; transition: all 0.2s; }}
div[data-testid="stButton"] > button:hover {{ background: #00e5ff; color: #000; box-shadow: 0 0 10px rgba(0, 229, 255, 0.4); }}
</style>
""", unsafe_allow_html=True)

# -------------------------
# CONSTANTS & SUPABASE API
# -------------------------
VENDOR_SCHEDULE = {
    "Monday": ["Old Dutch", "Coke", "Pepsi", "Frito Lay (Retail)", "Frito Lay (Vending)", "Italian Bakery", "Canada Bread"],
    "Tuesday": ["TGP", "Old Dutch", "Kenelli", "Frito Lay (Retail)"],
    "Wednesday": ["Old Dutch", "Frito Lay (Retail)"],
    "Thursday": ["TGP", "Old Dutch", "Pepsi", "Frito Lay (Vending)", "Italian Bakery", "Canada Bread"],
    "Friday": ["Old Dutch", "Coke", "Frito Lay (Retail)"],
    "Saturday": ["Italian Bakery"],
    "Sunday": ["TGP"]
}

PREMIUM_STAFF = ["Chris", "Ashley", "Luke", "Chandler"]
ORDER_LOCATIONS = ["1", "2", "3", "22"]
aisles = ["Aisle 1", "Aisle 2", "Aisle 3", "Aisle 4", "Aisle 5", "Aisle 6", "Aisle 7", "Aisle 8", "Receiving", "Freezer", "Bakery", "Outside"]

# Robust URL parameter parsing
query_params = st.query_params

def is_flag_active(key):
    """Returns True if the key exists in the URL, unless explicitly set to false/0/no"""
    if key in query_params:
        val = str(query_params.get(key, "")).lower()
        return val not in ["false", "0", "no"]
    return False

is_tv_url_mode = is_flag_active("tvmode")
is_tv_settings_mode = is_flag_active("settings")
is_cs_mode = str(query_params.get("mode", "")).lower() in ["cs", "desk", "service"]

# Native REST API Setup
try:
    supabase: Client = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
except Exception as e:
    st.error("Missing SUPABASE_URL or SUPABASE_KEY in secrets.")
    st.stop()

def to_df(data):
    df = pd.DataFrame(data)
    if not df.empty: df.columns = df.columns.str.lower()
    return df

# -------------------------
# DATA FETCHING (100% LIVE, NO CACHE)
# -------------------------
def load_fast_data():
    """Polls high-volatility tables via REST WITHOUT CACHE to ensure multi-device sync."""
    try:
        tasks = to_df(supabase.table("tasks").select("*").eq("status", "Open").execute().data)
        oos = to_df(supabase.table("oos").select("*").eq("status", "Open").execute().data)
        orders = to_df(supabase.table("special_orders").select("*").eq("status", "Open").execute().data)
        expected = to_df(supabase.table("expected_orders").select("*").eq("status", "Pending").execute().data)

        t_res = supabase.table("tasks").select("task_id, task_detail, time_closed, closed_by").eq("status", "Closed").neq("closed_by", "AUTO").order("time_closed", desc=True).limit(6).execute()
        o_res = supabase.table("oos").select("oos_id, zone, time_closed, closed_by").eq("status", "Closed").neq("closed_by", "AUTO").order("time_closed", desc=True).limit(6).execute()
        
        audits = []
        for t in t_res.data:
            if t.get("time_closed"): audits.append({"id": t.get("task_id"), "event": f"Task: {t.get('task_detail')}", "time": t.get("time_closed"), "user": t.get("closed_by"), "type": "task"})
        for o in o_res.data:
            if o.get("time_closed"): audits.append({"id": o.get("oos_id"), "event": f"Cleared Holes: {o.get('zone')}", "time": o.get("time_closed"), "user": o.get("closed_by"), "type": "oos"})
        
        a_df = pd.DataFrame(audits)
        if not a_df.empty: a_df = a_df.sort_values(by="time", ascending=False).head(6)

        return {"tasks": tasks, "oos": oos, "orders": orders, "expected": expected, "audit": a_df, "connection_error": False}
    except Exception as e:
        # Failsafe: Return empty dataframes and trigger the reconnect warning instead of crashing
        return {"tasks": pd.DataFrame(), "oos": pd.DataFrame(), "orders": pd.DataFrame(), "expected": pd.DataFrame(), "audit": pd.DataFrame(), "connection_error": True}

@st.cache_data(ttl=30)
def load_slow_data():
    """Polls low-volatility tables."""
    try:
        return {
            "counts": to_df(supabase.table("counts").select("*").eq("id", 1).execute().data),
            "staff": to_df(supabase.table("staff").select("*").execute().data),
            "settings": to_df(supabase.table("settings").select("*").execute().data),
            "ticker": to_df(supabase.table("ticker").select("*").execute().data)
        }
    except Exception:
        return {"counts": pd.DataFrame(), "staff": pd.DataFrame(), "settings": pd.DataFrame(), "ticker": pd.DataFrame()}

def load_historical_data(target_date_str=None):
    """Loads historical tasks with robust Edmonton-to-UTC timezone conversions."""
    try:
        if target_date_str:
            dt_obj = datetime.strptime(target_date_str, "%Y-%m-%d")
            if hasattr(LOCAL_TZ, 'localize'):
                start_local = LOCAL_TZ.localize(dt_obj)
            else:
                start_local = dt_obj.replace(tzinfo=LOCAL_TZ)
                
            end_local = start_local + timedelta(days=1, seconds=-1)
            start_iso = start_local.astimezone(timezone.utc).isoformat()
            end_iso = end_local.astimezone(timezone.utc).isoformat()
            
            tasks = to_df(supabase.table("tasks").select("*").in_("status", ["Closed", "Archived"]).gte("time_closed", start_iso).lte("time_closed", end_iso).execute().data)
            oos = to_df(supabase.table("oos").select("*").in_("status", ["Closed", "Archived"]).gte("time_closed", start_iso).lte("time_closed", end_iso).execute().data)
        else:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            tasks = to_df(supabase.table("tasks").select("*").in_("status", ["Closed", "Archived"]).gte("time_closed", cutoff).execute().data)
            oos = to_df(supabase.table("oos").select("*").in_("status", ["Closed", "Archived"]).gte("time_closed", cutoff).execute().data)
    except Exception:
        return {"tasks": pd.DataFrame(), "oos": pd.DataFrame()}
        
    if not tasks.empty:
        tasks['time_submitted'] = pd.to_datetime(tasks['time_submitted'], errors='coerce')
        tasks['time_closed'] = pd.to_datetime(tasks['time_closed'], errors='coerce')
        tasks['actual_mins'] = (tasks['time_closed'] - tasks['time_submitted']).dt.total_seconds() / 60.0
    return {"tasks": tasks, "oos": oos}

def clear_fast_cache(): 
    pass # Deprecated: Fast data is now 100% live
    
def clear_full_cache(): 
    load_slow_data.clear()

fast_data = load_fast_data()
slow_data = load_slow_data()

c_df = slow_data.get("counts", pd.DataFrame())
staff_df = slow_data.get("staff", pd.DataFrame())
set_df = slow_data.get("settings", pd.DataFrame())

g_pcs = int(c_df["grocery"].iloc[0]) if not c_df.empty else 0
f_pcs = int(c_df["frozen"].iloc[0]) if not c_df.empty else 0
staff_count = max(1, int(c_df["staff"].iloc[0])) if not c_df.empty else 1

master_staff = staff_df[staff_df["name"] != "Unassigned"]["name"].tolist() if not staff_df.empty else PREMIUM_STAFF
active_staff = staff_df[(staff_df["active"] == 1) & (staff_df["name"] != "Unassigned")]["name"].tolist() if not staff_df.empty else PREMIUM_STAFF

admin_pin_hash = ""
cases_per_hour = 55.0
start_time_str = "07:00"
end_time_str = "15:00"
shift_notes = ""
is_critical_alert = False
seasonal_progress = 0

if not set_df.empty:
    def get_set(name, default):
        v = set_df.loc[set_df["setting_name"] == name, "setting_value"]
        return v.iloc[0] if not v.empty else default
    
    admin_pin_hash = str(get_set("Admin_PIN", ""))
    cases_per_hour = float(get_set("Cases_Per_Hour", 55.0))
    start_time_str = str(get_set("Start_Time", "07:00"))
    end_time_str = str(get_set("End_Time", "15:00"))
    shift_notes = str(get_set("Shift_Notes", ""))
    is_critical_alert = str(get_set("Critical_Alert", "0")) == "1"
    seasonal_progress = int(get_set("Seasonal_Progress", 0))

# -------------------------
# NATIVE REST WRITE ACTIONS
# -------------------------
def assign_task(task_id, widget_key):
    staff = st.session_state.get(widget_key, "Unassigned")
    try:
        supabase.table("tasks").update({"assigned_to": str(staff)}).eq("task_id", str(task_id)).execute()
    except Exception as e:
        st.toast(f"Database blocked assignment to '{staff}'. Check your Staff table.", icon="🛑")

def complete_task(task_id, user):
    st.session_state["hidden_t"].append(str(task_id)) 
    try:
        supabase.table("tasks").update({"status": "Closed", "closed_by": user, "time_closed": utc_now_iso()}).eq("task_id", str(task_id)).execute()
    except Exception as e:
        pass

def complete_oos(oos_id, user):
    st.session_state["hidden_o"].append(str(oos_id))
    try:
        supabase.table("oos").update({"status": "Closed", "closed_by": user, "time_closed": utc_now_iso()}).eq("oos_id", str(oos_id)).execute()
    except Exception as e:
        pass

def complete_special_order(order_id, user):
    st.session_state["hidden_s"].append(str(order_id))
    try:
        supabase.table("special_orders").update({"status": "Closed", "closed_by": user, "time_closed": utc_now_iso()}).eq("order_id", str(order_id)).execute()
    except Exception as e:
        pass

def complete_expected_order(exp_id, user):
    st.session_state["hidden_e"].append(str(exp_id))
    try:
        supabase.table("expected_orders").update({"status": "Closed", "closed_by": user, "time_closed": utc_now_iso()}).eq("exp_id", str(exp_id)).execute()
    except Exception as e:
        pass

def undo_action(item_id, item_type):
    try:
        if item_type == "task":
            if item_id in st.session_state["hidden_t"]: st.session_state["hidden_t"].remove(item_id)
            supabase.table("tasks").update({"status": "Open", "closed_by": "", "time_closed": ""}).eq("task_id", str(item_id)).execute()
        elif item_type == "oos":
            if item_id in st.session_state["hidden_o"]: st.session_state["hidden_o"].remove(item_id)
            supabase.table("oos").update({"status": "Open", "closed_by": "", "time_closed": ""}).eq("oos_id", str(item_id)).execute()
    except Exception as e:
        pass

def execute_omni_command(cmd, user, is_quick_key=False):
    cmd_l = cmd.lower()
    pri = "Routine"
    if any(w in cmd_l for w in ["urgent", "spill", "fire", "code", "now", "rush"]): pri = "Urgent"
    elif any(w in cmd_l for w in ["high", "fast", "soon", "bale", "sweep"]): pri = "High"

    zone = "General"
    for z in aisles:
        if z.lower() in cmd_l: zone = z; break
    if "receiving" in cmd_l or "bale" in cmd_l: zone = "Receiving"

    desc = cmd.strip().upper()
    try:
        supabase.table("tasks").insert({
            "task_id": gen_id(), "task_detail": desc, "status": "Open", "priority": pri, 
            "zone": zone, "assigned_to": "Unassigned", "est_mins": 15, 
            "time_submitted": utc_now_iso(), "closed_by": "", "time_closed": ""
        }).execute()
        if not is_quick_key: st.toast(f"SYSTEM: Deploying '{desc}'", icon="⚙️")
    except Exception as e:
        st.toast("Failed to dispatch command. Duplicate or DB error.", icon="❌")

def load_daily_rhythm(grocery_pcs, frozen_pcs, staff_num, cph):
    hrs_math = (((grocery_pcs + frozen_pcs) / cph) / staff_num) * 60 if (grocery_pcs + frozen_pcs) > 0 else 120
    curr_date = yeg_now()
    curr_day = curr_date.strftime('%A')
    curr_month = curr_date.month
    
    ds = [
        {"Task": "Direction Huddle", "Priority": "Urgent", "Zone": "General", "Time": 5}, 
        {"Task": "Store Walk", "Priority": "High", "Zone": "General", "Time": 30},
        {"Task": "FIFO Audit (Pick 1 Random Aisle)", "Priority": "Routine", "Zone": "General", "Time": 15},
        {"Task": "Level off displays", "Priority": "Routine", "Zone": "General", "Time": 10}
    ]
    
    if curr_day in ["Sunday", "Tuesday", "Thursday"]:
        ds.append({"Task": "TGP Order", "Priority": "Urgent", "Zone": "Receiving", "Time": int(hrs_math)})
    else:
        for aisle_num in range(1, 9): ds.append({"Task": f"Back stock Aisle {aisle_num}", "Priority": "Routine", "Zone": f"Aisle {aisle_num}", "Time": 45})
        ds.append({"Task": "Back stock Freezer", "Priority": "Routine", "Zone": "Freezer", "Time": 45})
        ds.append({"Task": "Check items out of the air", "Priority": "Routine", "Zone": "General", "Time": 30})
    
    if curr_day == "Wednesday": ds.append({"Task": "PRIMARY AD CHANGEOVER", "Priority": "Urgent", "Zone": "General", "Time": 240})
    
    if curr_day == "Tuesday":
        ds.append({"Task": "MERCH ENGINE: Verify Dairy Kill Dates", "Priority": "High", "Zone": "Freezer", "Time": 30})
        ds.append({"Task": "MERCH ENGINE: Verify Bread Expirations", "Priority": "High", "Zone": "Bakery", "Time": 15})
        
    if curr_month in [11, 12, 1, 2, 3]:
        ds.append({"Task": "EDMONTON PROTOCOL: Salt Front Entrance", "Priority": "High", "Zone": "Outside", "Time": 10})
        ds.append({"Task": "EDMONTON PROTOCOL: Clear Snow from Back Stairs", "Priority": "High", "Zone": "Outside", "Time": 15})
    
    # Loop over inserts individually so unique index duplicates don't crash the whole batch
    for d in ds:
        try:
            supabase.table("tasks").insert({
                "task_id": gen_id(), "task_detail": d["Task"].upper(), "status": "Open", 
                "priority": d["Priority"], "zone": d["Zone"], "assigned_to": "Unassigned", 
                "est_mins": d["Time"], "time_submitted": utc_now_iso(), "closed_by": "", "time_closed": ""
            }).execute()
        except Exception:
            pass # Skips any item that violates the unique DB index
            
    for v in VENDOR_SCHEDULE.get(curr_day, []):
        try:
            supabase.table("expected_orders").insert({
                "exp_id": gen_id(), "vendor": v.upper(), "expected_day": curr_day, 
                "status": "Pending", "logged_by": "AUTO", "closed_by": "", "time_closed": ""
            }).execute()
        except Exception:
            pass

    st.toast("Rhythm Loaded", icon="📅")

def update_setting(name, value):
    try:
        supabase.table("settings").delete().eq("setting_name", name).execute()
        supabase.table("settings").insert({"setting_name": name, "setting_value": str(value)}).execute()
        clear_full_cache()
    except Exception:
        pass

# -------------------------
# SIDEBAR OPERATIONAL CONTROLS (HIDDEN IN CS MODE)
# -------------------------
if not is_cs_mode and not is_tv_settings_mode:
    with st.sidebar:
        conn_color = "#00ff00" if (datetime.now().second % 10) < 5 else "#00cc00"
        st.markdown(f"<div style='display:flex; align-items:center;'><div style='width:10px;height:10px;border-radius:50%;background-color:{conn_color};margin-right:10px;box-shadow:0 0 8px {conn_color};'></div><div style='color:#00e5ff; font-weight:300; letter-spacing:3px; font-size: 18px;'>UPLINK ACTIVE</div></div>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        
        tv_toggle = st.toggle("📺 Local TV Display Mode", key="tv_toggle")
        should_auto_refresh = is_tv_url_mode or tv_toggle

        active_op = st.selectbox("Operator:", PREMIUM_STAFF)

        @st.fragment
        def render_sidebar_tools():
            if not should_auto_refresh and not st.session_state.get("show_analytics", False):
                if st.button("🔄 Force Sync Now"):
                    st.rerun() 

            with st.form("omni_form", clear_on_submit=True):
                st.markdown("**System Command Line**", unsafe_allow_html=True)
                omni_cmd = st.text_input("Natural Language Entry", placeholder="e.g. urgent spill in aisle 4")
                if st.form_submit_button("Deploy Command") and omni_cmd.strip(): execute_omni_command(omni_cmd, active_op)
            
            c1, c2, c3 = st.columns(3)
            if c1.button("💦 Spill", use_container_width=True): execute_omni_command("Urgent Spill in General", active_op, True)
            if c2.button("📦 Bale", use_container_width=True): execute_omni_command("High Priority Make Cardboard Bale", active_op, True)
            if c3.button("🧹 Sweep", use_container_width=True): execute_omni_command("High Priority Store Safety Sweep", active_op, True)

            with st.expander("➕ Manual Task Override"):
                with st.form("manual_task", clear_on_submit=True):
                    m_task = st.text_input("Task Description")
                    m_pri = st.selectbox("Priority", ["Routine", "High", "Urgent"])
                    m_zone = st.selectbox("Zone", ["General"] + aisles)
                    m_time = st.number_input("Est. Mins", min_value=1, value=15)
                    if st.form_submit_button("Deploy Task") and m_task:
                        try:
                            supabase.table("tasks").insert({
                                "task_id": gen_id(), "task_detail": m_task.strip().upper(), 
                                "status": "Open", "priority": m_pri, "zone": m_zone, 
                                "assigned_to": "Unassigned", "est_mins": m_time, 
                                "time_submitted": utc_now_iso(), "closed_by": "", "time_closed": ""
                            }).execute()
                        except Exception:
                            st.error("Failed to save task.")

            st.divider()

            with st.expander("📝 Shift Notes & Ticker"):
                new_note = st.text_area("Pass the baton:", value=shift_notes, height=100)
                is_crit = st.checkbox("Mark as Critical Alert", value=is_critical_alert)
                if st.button("Save Handover Notes"):
                    update_setting("Shift_Notes", html.escape(new_note.strip()))
                    update_setting("Critical_Alert", "1" if is_crit else "0")
                    st.toast("Notes Updated", icon="✅")
                    st.rerun()
                
                st.markdown("<hr style='margin: 10px 0; border-color: #1f3b5c;'>", unsafe_allow_html=True)
                with st.form("ticker_form", clear_on_submit=True):
                    t_msg = st.text_input("Broadcast Live Ticker Message")
                    if st.form_submit_button("Send to Ticker") and t_msg:
                        try:
                            supabase.table("ticker").insert({"msg_id": gen_id(), "message": html.escape(t_msg.strip().upper())}).execute()
                            clear_full_cache()
                            st.rerun()
                        except Exception:
                            pass

            with st.expander("👥 Shift Roster Settings"):
                selected_active = st.multiselect("Active Today:", master_staff, default=active_staff)
                if st.button("Update Roster"):
                    try:
                        supabase.table("staff").update({"active": 0}).neq("name", "Unassigned").execute()
                        if selected_active: supabase.table("staff").update({"active": 1}).in_("name", selected_active).execute()
                        clear_full_cache()
                        st.rerun()
                    except Exception:
                        pass
                st.markdown("<hr style='margin: 10px 0; border-color: #1f3b5c;'>", unsafe_allow_html=True)
                new_staff = st.text_input("Add New Team Member")
                if st.button("Add to Database") and new_staff.strip():
                    try:
                        supabase.table("staff").insert({"name": new_staff.strip().title(), "active": 1}).execute()
                        clear_full_cache()
                        st.rerun()
                    except Exception:
                        pass

            with st.form("load_form"):
                c_time1, c_time2 = st.columns(2)
                in_arr = c_time1.time_input("Start of Order", value=datetime.strptime(start_time_str, "%H:%M").time())
                in_end = c_time2.time_input("End of Order", value=datetime.strptime(end_time_str, "%H:%M").time())
                
                c1, c2 = st.columns(2)
                in_g = c1.number_input("Groc Pcs", min_value=0, value=g_pcs)
                in_f = c2.number_input("Froz Pcs", min_value=0, value=f_pcs)
                in_s = st.number_input("Active Staff", min_value=1, value=staff_count)
                if st.form_submit_button("Calculate Labor"):
                    try:
                        supabase.table("counts").update({"grocery": in_g, "frozen": in_f, "staff": in_s, "last_update": utc_now_iso()}).eq("id", 1).execute()
                        update_setting("Start_Time", in_arr.strftime("%H:%M"))
                        update_setting("End_Time", in_end.strftime("%H:%M"))
                        st.rerun()
                    except Exception:
                        pass

            with st.form("oos_form", clear_on_submit=True):
                o_z = st.selectbox("Log Shelf Holes", aisles[:8] + ["Freezer", "Bakery"])
                c1, c2 = st.columns(2)
                o_c = c1.number_input("Qty Holes", min_value=1, value=1)
                o_n = c2.text_input("Notes")
                if st.form_submit_button("Log OOS"):
                    try:
                        supabase.table("oos").insert({
                            "oos_id": gen_id(), "zone": o_z, "hole_count": o_c, 
                            "notes": o_n.strip().upper(), "status": "Open", 
                            "logged_by": active_op, "time_logged": utc_now_iso(), "closed_by": "", "time_closed": ""
                        }).execute()
                    except Exception:
                        pass

            with st.expander("🛠️ Advanced Loggers"):
                with st.form("vendor_form", clear_on_submit=True):
                    e_ven = st.text_input("Log Expected Vendor")
                    if st.form_submit_button("Log") and e_ven:
                        try:
                            supabase.table("expected_orders").insert({
                                "exp_id": gen_id(), "vendor": e_ven.strip().upper(), 
                                "expected_day": yeg_now().strftime("%A"), "status": "Pending", 
                                "logged_by": active_op, "closed_by": "", "time_closed": ""
                            }).execute()
                        except Exception:
                            pass
                
                new_prog = st.slider("Seasonal Changeover %", 0, 100, seasonal_progress, step=10)
                if st.button("Update Tracker"):
                    update_setting("Seasonal_Progress", new_prog)
                    st.rerun()

            # --- LOCKED ADMIN CONSOLE ---
            st.divider()
            st.markdown("### 🔒 SYSTEM ADMIN")
            
            admin_pass = st.text_input("Admin PIN", type="password")
            
            if (admin_pin_hash == "") or (hash_pin(admin_pass) == admin_pin_hash):
                st.success("Admin Unlocked")
                
                if st.button("⬅️ Close Analytics" if st.session_state.get("show_analytics", False) else "📈 Launch Analytics", type="primary", use_container_width=True):
                    st.session_state["show_analytics"] = not st.session_state.get("show_analytics", False)
                    st.rerun()
                    
                with st.expander("⚙️ System Config"):
                    new_scale = st.slider("TV UI Scale (%)", 80, 150, font_scale, step=5)
                    if st.button("Save Scale"):
                        st.session_state["ui_font_scale"] = new_scale
                        st.rerun()
                        
                    st.divider()
                    st.caption("Database Health (Rows)")
                    try:
                        t_count = supabase.table("tasks").select("*", count="exact").execute().count
                        o_count = supabase.table("oos").select("*", count="exact").execute().count
                        st.code(f"Tasks Table: {t_count}\nOOS Table: {o_count}")
                    except: st.code("DB Read Error")
                    
                    if st.button("🚨 HARD REBOOT SERVER"):
                        clear_full_cache()
                        st.toast("Server Cache Flushed", icon="🔥")
                        st.rerun()

                with st.expander("🗄️ Data Archival & Pruning"):
                    cutoff_30 = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
                    try:
                        old_tasks = to_df(supabase.table("tasks").select("*").in_("status", ["Closed", "Archived"]).lt("time_closed", cutoff_30).execute().data)
                        old_oos = to_df(supabase.table("oos").select("*").in_("status", ["Closed", "Archived"]).lt("time_closed", cutoff_30).execute().data)
                        
                        if old_tasks.empty and old_oos.empty:
                            st.info("No records older than 30 days found.")
                        else:
                            st.success(f"Found {len(old_tasks)} old tasks.")
                            arch_buffer = io.BytesIO()
                            with pd.ExcelWriter(arch_buffer, engine='openpyxl') as writer:
                                if not old_tasks.empty: old_tasks.to_excel(writer, sheet_name='Archived_Tasks', index=False)
                                if not old_oos.empty: old_oos.to_excel(writer, sheet_name='Archived_OOS', index=False)
                            st.download_button("📥 1. Download Backup (Excel)", data=arch_buffer.getvalue(), file_name=f"TGP_Archive_{yeg_now().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                            st.warning("Download backup before purging!")
                            if st.button("🗑️ 2. Purge Database", type="primary", use_container_width=True):
                                supabase.table("tasks").delete().in_("status", ["Closed", "Archived"]).lt("time_closed", cutoff_30).execute()
                                supabase.table("oos").delete().in_("status", ["Closed", "Archived"]).lt("time_closed", cutoff_30).execute()
                                st.rerun()
                    except Exception:
                        st.error("Failed to load archival data.")

                with st.expander("🚨 Full Board Reset"):
                    if st.button("Execute EOD Sweep", type="primary"):
                        t_now = utc_now_iso()
                        try:
                            # 1. Archive Active Items as AUTO
                            supabase.table("tasks").update({"status": "Archived", "closed_by": "AUTO", "time_closed": t_now}).eq("status", "Open").execute()
                            supabase.table("oos").update({"status": "Archived", "closed_by": "AUTO", "time_closed": t_now}).eq("status", "Open").execute()
                            supabase.table("special_orders").update({"status": "Archived", "closed_by": "AUTO", "time_closed": t_now}).eq("status", "Open").execute()
                            supabase.table("expected_orders").update({"status": "Archived", "closed_by": "AUTO", "time_closed": t_now}).eq("status", "Pending").execute()
                            
                            # 2. Archive already closed items so they drop off the Audit Terminal
                            supabase.table("tasks").update({"status": "Archived"}).eq("status", "Closed").execute()
                            supabase.table("oos").update({"status": "Archived"}).eq("status", "Closed").execute()
                            supabase.table("special_orders").update({"status": "Archived"}).eq("status", "Closed").execute()
                            supabase.table("expected_orders").update({"status": "Archived"}).eq("status", "Closed").execute()
                            
                            try:
                                supabase.table("ticker").delete().neq("message", "xyz_impossible_match").execute()
                            except Exception:
                                pass
                            
                            # Flush the Optimistic UI Memory Banks
                            for key in ["hidden_t", "hidden_o", "hidden_s", "hidden_e"]:
                                st.session_state[key] = []
                                
                            clear_full_cache()
                            st.rerun()
                        except Exception:
                            pass

        render_sidebar_tools()

# -------------------------
# CUSTOMER SERVICE PORTAL
# -------------------------
def render_cs_desk():
    st.markdown("<div class='header-bar'><div class='header-title'>CUSTOMER SERVICE // SPECIAL ORDERS</div></div>", unsafe_allow_html=True)
    st.write("Log items requested by customers here. They will instantly appear on the Operations board on the floor.")

    with st.form("cs_order_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        c_name = c1.text_input("Customer Name (Required)")
        c_contact = c2.text_input("Contact Info (Phone/Email)")

        i1, i2 = st.columns([3, 1])
        c_item = i1.text_input("Item Description (Required)")
        c_loc = i2.selectbox("Order Location", ORDER_LOCATIONS)

        cs_rep = st.text_input("Your Name (CS Rep)")

        submit = st.form_submit_button("SEND TO FLOOR 🚀", use_container_width=True)

        if submit:
            if c_name and c_item and cs_rep:
                try:
                    supabase.table("special_orders").insert({
                        "order_id": gen_id(),
                        "customer": c_name.strip().upper(),
                        "item": c_item.strip().upper(),
                        "contact": c_contact.strip().upper(),
                        "location": c_loc,
                        "status": "Open",
                        "logged_by": cs_rep.strip().upper(),
                        "time_logged": utc_now_iso(),
                        "closed_by": "",
                        "time_closed": ""
                    }).execute()
                    st.success(f"✅ Order for {c_name} has been sent to the floor!")
                except Exception as e:
                    st.error("Failed to send order. Check connection.")
            else:
                st.warning("⚠️ Please fill out the Customer Name, Item Description, and Your Name.")

# -------------------------
# SECRET TV SETTINGS MENU
# -------------------------
def render_tv_settings():
    st.markdown("<div class='header-bar'><div class='header-title'>TV MODE // DISPLAY SETTINGS</div></div>", unsafe_allow_html=True)
    st.info("⏸️ Auto-scroll is currently paused. Adjust the zoom level below.")
    
    new_scale = st.slider("TV UI Scale (%)", 80, 200, font_scale, step=5)
    
    st.markdown("<br>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    if c1.button("✅ Save & Return to Board", type="primary", use_container_width=True):
        st.session_state["ui_font_scale"] = new_scale
        if "settings" in st.query_params: del st.query_params["settings"]
        st.rerun()
        
    if c2.button("❌ Cancel", use_container_width=True):
        if "settings" in st.query_params: del st.query_params["settings"]
        st.rerun()

# -------------------------
# ANALYTICS (TIME MACHINE)
# -------------------------
def render_analytics():
    st.markdown("## 📊 SYSTEM ANALYTICS")
    
    c1, c2 = st.columns([0.3, 0.7])
    target_date = c1.date_input("Time Machine: Select Date", value=yeg_now().date())
    
    history = load_historical_data(target_date.strftime("%Y-%m-%d"))
    t_df = history["tasks"]
    o_df = history["oos"]
    
    if t_df.empty or o_df.empty:
        st.warning(f"No closed data found for {target_date.strftime('%B %d, %Y')}.")
        return

    st.divider()
    m1, m2, m3 = st.columns(3)
    m1.metric("Tasks Completed", len(t_df))
    m2.metric("Avg Time to Close (Mins)", round(t_df['actual_mins'].mean(), 1))
    m3.metric("Total Shelf Holes Logged", o_df['hole_count'].sum() if 'hole_count' in o_df else 0)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        t_df_clean = t_df.drop(columns=['time_submitted', 'time_closed'], errors='ignore') 
        t_df_clean.to_excel(writer, sheet_name='Closed_Tasks', index=False)
        o_df.to_excel(writer, sheet_name='Closed_OOS', index=False)
    
    st.download_button("📥 Export Report to Excel", data=buffer.getvalue(), file_name=f"TGP_Report_{target_date.strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.subheader("Staff Throughput")
    if 'closed_by' in t_df.columns:
        staff_counts = t_df.groupby("closed_by").size().reset_index(name='tasks_closed').sort_values(by='tasks_closed', ascending=False)
        st.bar_chart(staff_counts, x="closed_by", y="tasks_closed", color="#00e5ff")

# -------------------------
# BOARD RENDER LAYER 
# -------------------------
def render_main_board(fast_snap, is_tv):
    # Check for critical connection errors before rendering
    if fast_snap.get("connection_error", False):
        st.markdown("<div class='alert-banner'>📡 CONNECTION LOST: Retrying database uplink...</div>", unsafe_allow_html=True)
        
    t_df = fast_snap.get("tasks", pd.DataFrame()).copy()
    oos_df = fast_snap.get("oos", pd.DataFrame()).copy()
    s_df = fast_snap.get("orders", pd.DataFrame()).copy()
    e_df = fast_snap.get("expected", pd.DataFrame()).copy()
    a_df = fast_snap.get("audit", pd.DataFrame()).copy()
    
    # OPTIMISTIC UI FILTER
    hidden_t = st.session_state.get("hidden_t", [])
    if hidden_t and not t_df.empty: t_df = t_df[~t_df['task_id'].astype(str).isin(hidden_t)]
    hidden_o = st.session_state.get("hidden_o", [])
    if hidden_o and not oos_df.empty: oos_df = oos_df[~oos_df['oos_id'].astype(str).isin(hidden_o)]
    hidden_s = st.session_state.get("hidden_s", [])
    if hidden_s and not s_df.empty: s_df = s_df[~s_df['order_id'].astype(str).isin(hidden_s)]
    hidden_e = st.session_state.get("hidden_e", [])
    if hidden_e and not e_df.empty: e_df = e_df[~e_df['exp_id'].astype(str).isin(hidden_e)]
    
    curr_now = yeg_now()
    now_utc = datetime.now(timezone.utc)
    
    # SHIFT NOTES & SEASONAL PROGRESS
    if is_critical_alert and shift_notes:
        st.markdown(f"<div class='alert-banner'>🚨 CRITICAL ALERT: {shift_notes}</div>", unsafe_allow_html=True)
    elif shift_notes:
        st.markdown(f"<div class='shift-note'>📌 <strong>HANDOVER NOTES:</strong> {shift_notes}</div>", unsafe_allow_html=True)

    if seasonal_progress > 0 and seasonal_progress < 100:
        st.progress(seasonal_progress / 100.0, text=f"Seasonal Changeover Progress: {seasonal_progress}%")

    # THE SECRET TV MENU BUTTON IS EMBEDDED DIRECTLY OVER THE ORANGE BAR HERE
    tv_secret_html = "<a href='?tvmode=true&settings=true' target='_self' class='secret-tv-btn' title='Open TV Settings'></a>" if is_tv else ""

    st.markdown(f"<div class='header-bar'>{tv_secret_html}<div class='header-title'>TGP CENTRE STORE // {curr_now.strftime('%A')}</div><div class='header-time'>{curr_now.strftime('%b %d, %Y')} | {curr_now.strftime('%H:%M')}</div></div>", unsafe_allow_html=True)

    # LABOR MATH
    g, f = g_pcs, f_pcs
    total_pcs = g + f
    t_mins = pd.to_numeric(t_df["est_mins"], errors='coerce').fillna(15).sum() if not t_df.empty else 0
    
    # Target Finish Math (Standard ETA)
    f_hrs = (total_pcs / cases_per_hour)
    total_hrs = (f_hrs + (t_mins / 60.0)) / staff_count
    
    arr_time_obj = datetime.strptime(start_time_str, "%H:%M").time()
    end_time_obj = datetime.strptime(end_time_str, "%H:%M").time()
    
    anchor_time = curr_now.replace(hour=arr_time_obj.hour, minute=arr_time_obj.minute, second=0, microsecond=0)
    end_anchor = curr_now.replace(hour=end_time_obj.hour, minute=end_time_obj.minute, second=0, microsecond=0)
    if end_anchor <= anchor_time: end_anchor += timedelta(days=1)
    
    eta = (anchor_time + timedelta(hours=total_hrs)).strftime('%H:%M') if (total_pcs > 0 or t_mins > 0) else "N/A"

    # Required Speed Math (Actual Piece Count Gauge)
    available_hrs = (end_anchor - anchor_time).total_seconds() / 3600.0
    task_hrs_per_staff = (t_mins / 60.0) / staff_count if staff_count > 0 else 0
    piece_hrs_avail = available_hrs - task_hrs_per_staff
    
    if total_pcs == 0:
        req_cph_display = "0/h"
        cph_css = ""
    elif piece_hrs_avail > 0:
        req_cph = (total_pcs / staff_count) / piece_hrs_avail
        req_cph_display = f"{int(req_cph)}/h"
        cph_css = "urgent" if req_cph > (cases_per_hour + 15) else "amber" if req_cph > cases_per_hour else ""
    else:
        req_cph_display = "MAX"
        cph_css = "urgent"

    st.markdown(f"<div class='kpi-container'><div class='kpi-box'><div class='kpi-label'>Grocery</div><div class='kpi-value'>{g}</div></div><div class='kpi-box'><div class='kpi-label'>Frozen</div><div class='kpi-value'>{f}</div></div><div class='kpi-box'><div class='kpi-label'>Staff</div><div class='kpi-value'>{staff_count}</div></div><div class='kpi-box'><div class='kpi-label'>Tasks</div><div class='kpi-value'>{int(t_mins)}m</div></div><div class='kpi-box'><div class='kpi-label'>Target End</div><div class='kpi-value'>{end_time_str}</div></div><div class='kpi-box'><div class='kpi-label'>Est. Finish</div><div class='kpi-value'>{eta}</div></div><div class='kpi-box {cph_css}'><div class='kpi-label'>Req. Speed</div><div class='kpi-value'>{req_cph_display}</div></div></div>", unsafe_allow_html=True)

    L, R = st.columns([0.65, 0.35], gap="small")
    
    with L:
        st.markdown("<div class='sect-header'>Active Directives</div>", unsafe_allow_html=True)
        
        surge_active = (total_hrs > 7.5) or (cph_css == "urgent")
        if surge_active: st.error("⚠️ SURGE MODE: Routine tasks suppressed. High operational velocity required.")
        
        if t_df.empty: 
            st.success("All tasks complete!")
        else:
            t_df['time_submitted_dt'] = pd.to_datetime(t_df['time_submitted'], utc=True, errors='coerce')
            t_df['age_mins'] = (now_utc - t_df['time_submitted_dt']).dt.total_seconds() / 60.0
            t_df['age_mins'] = t_df['age_mins'].fillna(0)
            
            if surge_active: t_df = t_df[(t_df['priority'] != 'Routine') | (t_df['age_mins'] > 90)]
                
            t_df['p_rank'] = t_df['priority'].map({"Urgent": 1, "High": 2, "Routine": 3}).fillna(3)
            t_df = t_df.sort_values(['p_rank', 'age_mins'], ascending=[True, False])
            
            for _, r in t_df.iterrows():
                age = r['age_mins']
                p_class = "data-routine"
                age_str = f"T+{int(age)}m" if age > 5 else "NEW"
                
                if r['priority'] == 'Urgent' or age > 90: p_class = "data-urgent"
                elif r['priority'] == 'High' or age > 45: p_class = "data-high"

                card_html = f"<div class='data-card {p_class}'><div><span class='card-zone'>[{r['zone']}]</span> {html.escape(r['task_detail'])}</div><div class='card-meta'><strong>{r['assigned_to']}</strong><br>{age_str}</div></div>"
                
                if is_tv:
                    st.markdown(card_html, unsafe_allow_html=True)
                else:
                    c1, c2, c3 = st.columns([0.65, 0.20, 0.15], gap="small")
                    c1.markdown(card_html, unsafe_allow_html=True)
                    
                    # Safely merge and strip all duplicate names out of the drop-down menu so Streamlit doesn't crash
                    base_opts = []
                    for x in ["Unassigned", "ALL STAFF"] + active_staff + [str(r['assigned_to'])]:
                        if x not in base_opts:
                            base_opts.append(x)
                            
                    c2.selectbox("Assign", base_opts, index=base_opts.index(str(r['assigned_to'])), key=f"sel_{r['task_id']}", label_visibility="collapsed", on_change=assign_task, args=(r['task_id'], f"sel_{r['task_id']}"))
                    c3.button("DONE", key=f"dn_{r['task_id']}", on_click=complete_task, args=(r['task_id'], active_op))

        st.markdown("<div class='sect-header' style='margin-top: 20px;'>Live Audit Terminal (Failsafe)</div>", unsafe_allow_html=True)
        if a_df.empty:
            st.markdown("<div class='terminal-box'><div class='term-line'>AWAITING SYSTEM DATA...</div></div>", unsafe_allow_html=True)
        else:
            term_html = "<div class='terminal-box'>"
            for _, r in a_df.iterrows():
                try: dt_str = datetime.fromisoformat(str(r['time'])).astimezone(LOCAL_TZ).strftime('%H:%M:%S')
                except: dt_str = "00:00:00"
                
                term_html += f"<div class='term-line'><div class='term-content'><span class='term-time'>[{dt_str}]</span><span class='term-user'>{html.escape(str(r['user']))}:</span> {html.escape(str(r['event']))}</div>"
                term_html += "</div>"
            term_html += "</div>"
            st.markdown(term_html, unsafe_allow_html=True)
            
            if not is_tv:
                st.caption("Failsafe: Reopen recent items")
                ucols = st.columns(min(len(a_df), 6))
                for i, (_, r) in enumerate(a_df.head(6).iterrows()):
                    trunc_name = str(r['event'])[:10] + ".."
                    ucols[i].button(f"↩️ {trunc_name}", key=f"undo_{r['id']}", help="Reopen", on_click=undo_action, args=(r['id'], r['type']))

    with R:
        st.markdown("<div class='sect-header'>Inventory Flags (OOS)</div>", unsafe_allow_html=True)
        if oos_df.empty: st.caption("No inventory anomalies reported.")
        for _, r in oos_df.iterrows():
            oos_html = f"<div class='data-card data-urgent'><div><span class='card-zone'>{r['zone']}</span>: {r['hole_count']} HOLES</div><div class='card-meta'>{html.escape(r.get('notes', ''))}</div></div>"
            if is_tv: st.markdown(oos_html, unsafe_allow_html=True)
            else:
                c1, c2 = st.columns([0.80, 0.20], gap="small")
                c1.markdown(oos_html, unsafe_allow_html=True)
                c2.button("CLR", key=f"o_{r['oos_id']}", on_click=complete_oos, args=(r['oos_id'], active_op))

        st.markdown("<div class='sect-header'>Incoming Customer Orders</div>", unsafe_allow_html=True)
        if s_df.empty: st.caption("No pending customer orders.")
        for _, r in s_df.iterrows():
            c1, c2 = st.columns([0.80, 0.20], gap="small")
            c1.markdown(f"<div class='data-card' style='border-left-color:#a855f7;'><div><span class='card-zone'>L:{r['location']}</span> {html.escape(r['item'])}</div><div class='card-meta'>{html.escape(r.get('customer', ''))}</div></div>", unsafe_allow_html=True)
            if not is_tv: c2.button("DONE", key=f"s_{r['order_id']}", on_click=complete_special_order, args=(r['order_id'], active_op))
                
        st.markdown("<div class='sect-header'>Vendor Deliveries</div>", unsafe_allow_html=True)
        if e_df.empty: st.caption("No pending vendor deliveries.")
        for _, r in e_df.iterrows():
            c1, c2 = st.columns([0.80, 0.20], gap="small")
            c1.markdown(f"<div class='data-card' style='border-left-color:#ffaa00;'>🚚 <span class='card-zone'>{html.escape(r['vendor'])}</span></div>", unsafe_allow_html=True)
            if not is_tv: c2.button("RCV", key=f"e_{r['exp_id']}", on_click=complete_expected_order, args=(r['exp_id'], active_op))

        st.divider()
        if not is_tv:
            if st.button("🚀 Load Daily Rhythm", use_container_width=True):
                load_daily_rhythm(g, f, staff_count, cases_per_hour)
                st.rerun()

    tk_df = slow_data.get("ticker", pd.DataFrame())
    if not tk_df.empty:
        live_tk = tk_df.dropna(subset=["message"])
        if not live_tk.empty:
            m_str = " &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; ".join(live_tk["message"].tolist())
            st.markdown(f"<div class='ticker-wrap'><div class='ticker'>📢 {m_str} &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; </div></div>", unsafe_allow_html=True)

# -------------------------
# JS INJECTIONS (TV SCROLL)
# -------------------------
# Completely block auto-scroll if the TV settings menu is open or in CS mode
if is_tv_url_mode and not is_tv_settings_mode and not is_cs_mode:
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
    if(window.top === window.self) scrollApp(); 
    </script>
    """, unsafe_allow_html=True)

# -------------------------
# ENTRYPOINT & LOGIC
# -------------------------
if is_cs_mode:
    render_cs_desk()
elif is_tv_settings_mode:
    render_tv_settings()
elif st.session_state.get("show_analytics", False):
    render_analytics()
else:
    if should_auto_refresh:
        @st.fragment(run_every=4)
        def tv_loop():
            render_main_board(load_fast_data(), is_tv=True)
        tv_loop()
    else:
        @st.fragment(run_every=10)
        def interactive_loop():
            render_main_board(load_fast_data(), is_tv=False)
        interactive_loop()
