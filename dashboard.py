import streamlit as st
import pandas as pd
import sqlite3
import html
import uuid
import hashlib
import time
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Edmonton")
except ImportError:
    import pytz
    LOCAL_TZ = pytz.timezone("America/Edmonton")

# --- CORE UTILITIES ---
def get_utc_now(): return datetime.now(timezone.utc).isoformat()
def get_local_now(): return datetime.now(LOCAL_TZ)
def get_pin_hash(pin_str): return hashlib.sha256(str(pin_str).encode()).hexdigest()
def gen_id(): return str(uuid.uuid4().hex)

DB_FILE = "tgp_board.db"

# --- UI CONFIG ---
st.set_page_config(page_title="TGP Comm Board", layout="wide", initial_sidebar_state="collapsed")

VENDOR_SCHEDULE = {
    "Monday": ["Old Dutch", "Coke", "Pepsi", "Frito Lay (Retail)", "Frito Lay (Vending)", "Italian Bakery", "Canada Bread"],
    "Tuesday": ["TGP", "Old Dutch"],
    "Wednesday": ["Old Dutch", "Frito Lay (Retail)"],
    "Thursday": ["TGP", "Old Dutch", "Pepsi", "Frito Lay (Vending)", "Italian Bakery", "Canada Bread"],
    "Friday": ["Old Dutch", "Coke", "Frito Lay (Retail)"],
    "Saturday": [],
    "Sunday": ["TGP"]
}

PREMIUM_STAFF = ["Chris", "Ashley", "Luke", "Chandler"]
ORDER_LOCATIONS = ["1", "2", "3", "22"]

# --- DATABASE ENGINE ---
def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=15.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS staff (Name TEXT PRIMARY KEY, Active INTEGER)")
        conn.execute('''CREATE TABLE IF NOT EXISTS tasks (Task_ID TEXT PRIMARY KEY, Task_Detail TEXT, Status TEXT, Priority TEXT, 
                         Zone TEXT, Assigned_To TEXT REFERENCES staff(Name) ON DELETE RESTRICT, 
                         Est_Mins INTEGER, Time_Submitted TEXT, Closed_By TEXT, Time_Closed TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS oos (OOS_ID TEXT PRIMARY KEY, Zone TEXT, Hole_Count INTEGER, Notes TEXT, 
                         Status TEXT, Logged_By TEXT, Time_Logged TEXT, Closed_By TEXT, Time_Closed TEXT)''')
        try: conn.execute("SELECT ID FROM counts")
        except sqlite3.OperationalError: conn.execute("DROP TABLE IF EXISTS counts")
        conn.execute('''CREATE TABLE IF NOT EXISTS counts (ID INTEGER PRIMARY KEY CHECK (ID = 1), Grocery INTEGER, Frozen INTEGER, 
                         Staff INTEGER CHECK (Staff >= 1), Last_Update TEXT, Weather_Alert INTEGER, Ticker_Msg TEXT)''')
        conn.execute("CREATE TABLE IF NOT EXISTS audit (Log_ID TEXT PRIMARY KEY, Timestamp TEXT, Event TEXT)")
        conn.execute('''CREATE TABLE IF NOT EXISTS special_orders (Order_ID TEXT PRIMARY KEY, Customer TEXT, Item TEXT, Contact TEXT, 
                         Location TEXT, Status TEXT, Logged_By TEXT, Time_Logged TEXT, Closed_By TEXT, Time_Closed TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS expected_orders (Exp_ID TEXT PRIMARY KEY, Vendor TEXT, Expected_Day TEXT, Status TEXT, 
                         Logged_By TEXT, Closed_By TEXT, Time_Closed TEXT)''')
        conn.execute("CREATE TABLE IF NOT EXISTS ticker (Msg_ID TEXT PRIMARY KEY, Message TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS settings (Setting_Name TEXT PRIMARY KEY, Setting_Value TEXT)")

        # Indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(Status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_oos_status ON oos(Status)")

        # Initial Seeds
        if conn.execute("SELECT COUNT(*) FROM counts").fetchone()[0] == 0:
            conn.execute("INSERT INTO counts VALUES (1, 0, 0, 1, ?, 0, '')", (get_utc_now(),))
        if conn.execute("SELECT COUNT(*) FROM staff").fetchone()[0] == 0:
            conn.executemany("INSERT INTO staff VALUES (?, ?)", [("Chris", 1), ("Ashley", 1), ("Luke", 1), ("Unassigned", 1)])
        if conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0] == 0:
            conn.executemany("INSERT INTO settings VALUES (?, ?)", [
                ("Cases_Per_Hour", "55"), 
                ("Admin_PIN", get_pin_hash("1234")),
                ("Global_TV_Mode", "0")
            ])

init_db()

# --- SERVICE LAYER (ATOMIC TRANSACTIONS) ---
def _internal_audit(cur, event):
    cur.execute("INSERT INTO audit VALUES (?, ?, ?)", (gen_id(), get_utc_now(), event))
    cur.execute("DELETE FROM audit WHERE rowid NOT IN (SELECT rowid FROM audit ORDER BY rowid DESC LIMIT 100)")

def _strict_update(cur, query, params):
    cur.execute(query, params)
    if cur.rowcount != 1: raise ValueError("Sync Error: Row not found.")

def assign_task(task_id, staff_name):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE tasks SET Assigned_To = ? WHERE Task_ID = ?", (staff_name, str(task_id)))
        _internal_audit(cur, f"Task assigned to {staff_name}")

def handle_assign_callback(task_id, widget_key):
    new_owner = st.session_state.get(widget_key, "Unassigned")
    try: assign_task(task_id, new_owner)
    except Exception as e: st.error(str(e))

def complete_task(task_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE tasks SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE Task_ID = ?", (user, get_utc_now(), str(task_id)))
        _internal_audit(cur, f"Task {task_id} cleared")

def complete_oos(oos_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE oos SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE OOS_ID = ?", (user, get_utc_now(), str(oos_id)))
        _internal_audit(cur, "OOS cleared")

def complete_special_order(order_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE special_orders SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE Order_ID = ?", (user, get_utc_now(), str(order_id)))
        _internal_audit(cur, "Customer Order Picked Up")

def complete_expected_order(exp_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE expected_orders SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE Exp_ID = ?", (user, get_utc_now(), str(exp_id)))
        _internal_audit(cur, "Vendor Freight Received")

# --- UI STYLING ---
st.markdown("""
<style>
footer { visibility: hidden; }
.stApp { background-color: #0b0f14; color: #d1d5db; }
header[data-testid="stHeader"] { background: rgba(0,0,0,0); visibility: visible; }
.block-container { padding-top: 1rem; }
.header-bar { display: flex; align-items: center; border-bottom: 3px solid #38bdf8; margin-bottom: 15px; padding-bottom: 10px; }
.header-title { font-size: 28px; font-weight: 800; color: #f9fafb; flex-grow: 1; text-transform: uppercase; }
.kpi-container { display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; margin-bottom: 20px; }
.kpi-box { background: #161b22; border-top: 4px solid #38bdf8; padding: 10px; border-radius: 4px; text-align:center; }
.kpi-box.urgent { border-top-color: #ef4444; }
.data-card { background: #161b22; border-left: 5px solid #38bdf8; padding: 10px; margin-bottom: 8px; border-radius: 4px; }
.data-urgent { border-left-color: #ef4444; }
.sect-header { font-size: 16px; font-weight: 700; color: #38bdf8; border-bottom: 1px solid #30363d; padding-bottom: 5px; margin: 15px 0 10px 0; text-transform: uppercase; }
.ticker-wrap { width: 100%; overflow: hidden; background-color: #facc15; padding: 10px 0; position: fixed; bottom: 0; left: 0; z-index: 999; }
.ticker { display: inline-block; white-space: nowrap; padding-left: 100%; animation: ticker 30s linear infinite; color: #000; font-family: 'Arial Black', sans-serif; font-size: 18px; }
@keyframes ticker { 0% { transform: translate3d(0, 0, 0); } 100% { transform: translate3d(-100%, 0, 0); } }
div[data-testid="stButton"] > button { border-radius: 4px; border: 1px solid #30363d; background: #21262d; color: #c9d1d9; font-weight: 700; width: 100%; }
</style>
""", unsafe_allow_html=True)

# --- GET PARAMETERS / GLOBAL STATE ---
query_params = st.query_params
is_url_tv_mode = query_params.get("mode") == "tv"

with get_db() as conn:
    set_df = pd.read_sql("SELECT * FROM settings", conn)
    staff_df = pd.read_sql("SELECT * FROM staff", conn)
    c_df = pd.read_sql("SELECT * FROM counts WHERE ID = 1", conn)

is_db_tv_on = False
if not set_df.empty:
    val = set_df.loc[set_df["Setting_Name"] == "Global_TV_Mode", "Setting_Value"]
    if not val.empty: is_db_tv_on = (val.iloc[0] == "1")

# Should we refresh every 2s? (If URL says so OR DB says so)
should_auto_refresh = is_url_tv_mode or is_db_tv_on

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("### 🔧 OPERATIONAL COMMAND")
    
    # Master Remote Control
    if is_db_tv_on:
        if st.button("🔴 DISABLE AUTO-REFRESH", type="primary"):
            with get_db() as conn: conn.execute("UPDATE settings SET Setting_Value = '0' WHERE Setting_Name = 'Global_TV_Mode'")
            st.rerun()
    else:
        if st.button("🚀 ENABLE GLOBAL AUTO-REFRESH", type="secondary"):
            with get_db() as conn: conn.execute("UPDATE settings SET Setting_Value = '1' WHERE Setting_Name = 'Global_TV_Mode'")
            st.rerun()

    st.divider()
    active_op = st.selectbox("Operator:", PREMIUM_STAFF)
    
    with st.expander("👥 Roster Management"):
        master_list = staff_df[staff_df["Name"] != "Unassigned"]["Name"].tolist()
        active_list = staff_df[(staff_df["Active"] == 1) & (staff_df["Name"] != "Unassigned")]["Name"].tolist()
        new_active = st.multiselect("Active Today:", master_list, default=active_list)
        if set(new_active) != set(active_list):
            with get_db() as conn:
                conn.execute("UPDATE staff SET Active = 0 WHERE Name != 'Unassigned'")
                if new_active:
                    placeholders = ','.join(['?'] * len(new_active))
                    conn.execute(f"UPDATE staff SET Active = 1 WHERE Name IN ({placeholders})", new_active)
            st.rerun()

    st.divider()
    with st.form("load_entry"):
        st.markdown("**Freight Entry**")
        g_in = st.number_input("Grocery", min_value=0, value=int(c_df["Grocery"].iloc[0]))
        f_in = st.number_input("Frozen", min_value=0, value=int(c_df["Frozen"].iloc[0]))
        s_in = st.number_input("Staff Count", min_value=1, value=int(c_df["Staff"].iloc[0]))
        if st.form_submit_button("Update Load"):
            with get_db() as conn: conn.execute("UPDATE counts SET Grocery=?, Frozen=?, Staff=?, Last_Update=? WHERE ID=1", (g_in, f_in, s_in, get_utc_now()))
            st.rerun()

    if st.button("🌦️ Toggle Weather Alert"):
        with get_db() as conn: conn.execute("UPDATE counts SET Weather_Alert = CASE WHEN Weather_Alert=1 THEN 0 ELSE 1 END WHERE ID=1")
        st.rerun()

    if st.button("🌙 EOD RESET", type="primary"):
        execute_eod_reset(); st.rerun()

# --- MAIN RENDER CYCLE ---
def render_full_board():
    with get_db() as conn:
        t_df = pd.read_sql("SELECT * FROM tasks WHERE Status = 'Open'", conn)
        oos_df = pd.read_sql("SELECT * FROM oos WHERE Status = 'Open'", conn)
        s_df = pd.read_sql("SELECT * FROM special_orders WHERE Status = 'Open'", conn)
        e_df = pd.read_sql("SELECT * FROM expected_orders WHERE Status = 'Pending'", conn)
        tk_df = pd.read_sql("SELECT * FROM ticker", conn)
        counts = pd.read_sql("SELECT * FROM counts WHERE ID = 1", conn).iloc[0]
        staff_live = pd.read_sql("SELECT * FROM staff WHERE Active = 1", conn)["Name"].tolist()
        cph = float(set_df.loc[set_df["Setting_Name"] == "Cases_Per_Hour", "Setting_Value"].iloc[0])

    now = get_local_now()
    
    # UI Header
    st.markdown(f"<div class='header-bar'><div class='header-title'>TGP CENTRE STORE // {now.strftime('%A')}</div><div style='font-size:32px; font-weight:bold;'>{now.strftime('%I:%M %p')}</div></div>", unsafe_allow_html=True)
    
    if should_auto_refresh:
        st.caption("✅ 2-Second Live Sync Active")

    # KPI Calculation
    total_pcs = counts['Grocery'] + counts['Frozen']
    task_mins = t_df["Est_Mins"].sum()
    staff_num = max(1, counts['Staff'])
    hrs_needed = ((total_pcs / cph) + (task_mins / 60.0)) / staff_num
    eta = (now + timedelta(hours=hrs_needed)).strftime('%I:%M %p') if (total_pcs > 0 or task_mins > 0) else "N/A"

    st.markdown(f"""
    <div class='kpi-container'>
        <div class='kpi-box'><div class='kpi-label'>Load Total</div><div class='kpi-value'>{total_pcs} Pcs</div></div>
        <div class='kpi-box'><div class='kpi-label'>Active Staff</div><div class='kpi-value'>{staff_num}</div></div>
        <div class='kpi-box'><div class='kpi-label'>Task Mins</div><div class='kpi-value'>{int(task_mins)}m</div></div>
        <div class='kpi-box {'urgent' if hrs_needed > 7.5 else ''}'><div class='kpi-label'>Hrs Needed</div><div class='kpi-value'>{round(hrs_needed,1)}h</div></div>
        <div class='kpi-box'><div class='kpi-label'>True ETA</div><div class='kpi-value' style='color:#00e676;'>{eta}</div></div>
        <div class='kpi-box {'urgent' if counts['Weather_Alert'] else ''}'><div class='kpi-label'>Weather</div><div class='kpi-value'>{'SNOW' if counts['Weather_Alert'] else 'CLEAR'}</div></div>
    </div>
    """, unsafe_allow_html=True)

    col_L, col_R = st.columns([0.65, 0.35])

    with col_L:
        st.markdown("<div class='sect-header'>Active Shift Tasks</div>", unsafe_allow_html=True)
        if t_df.empty: st.success("ALL CLEAR")
        for _, r in t_df.iterrows():
            c1, c2, c3 = st.columns([0.6, 0.25, 0.15])
            u_class = "data-urgent" if r['Priority'] == "Urgent" else ""
            c1.markdown(f"<div class='data-card {u_class}'><strong>[{r['Zone']}]</strong> {r['Task_Detail']} ({r['Est_Mins']}m)<br><small>OWNER: {r['Assigned_To']}</small></div>", unsafe_allow_html=True)
            opts = ["Unassigned"] + [s for s in staff_live if s != "Unassigned"]
            if r['Assigned_To'] not in opts: opts.append(r['Assigned_To'])
            c2.selectbox("Ass", opts, index=opts.index(r['Assigned_To']), key=f"sel_{r['Task_ID']}", label_visibility="collapsed", on_change=handle_assign_callback, args=(r['Task_ID'], f"sel_{r['Task_ID']}"))
            c3.button("DONE", key=f"dn_{r['Task_ID']}", on_click=complete_task, args=(r['Task_ID'], active_op))

        st.markdown("<div class='sect-header'>Orders & Freight</div>", unsafe_allow_html=True)
        cL, cR = st.columns(2)
        with cL:
            st.caption("Customer Orders")
            for _, r in s_df.iterrows():
                st.markdown(f"<div class='data-card' style='border-left-color:#a855f7;'><strong>Loc {r['Location']}</strong>: {r['Item']}<br><small>{r['Customer']}</small></div>", unsafe_allow_html=True)
                st.button("PU", key=f"s_{r['Order_ID']}", on_click=complete_special_order, args=(r['Order_ID'], active_op))
        with cR:
            st.caption("Vendor Freight")
            for _, r in e_df.iterrows():
                st.markdown(f"<div class='data-card' style='border-left-color:#f59e0b;'>🚚 <strong>{r['Vendor']}</strong></div>", unsafe_allow_html=True)
                st.button("RCV", key=f"e_{r['Exp_ID']}", on_click=complete_expected_order, args=(r['Exp_ID'], active_op))

    with col_R:
        st.markdown("<div class='sect-header'>Shelf Holes (OOS)</div>", unsafe_allow_html=True)
        if oos_df.empty: st.caption("No holes logged.")
        for _, r in oos_df.iterrows():
            c1, c2 = st.columns([0.8, 0.2])
            c1.markdown(f"<div class='data-card data-urgent'><strong>{r['Zone']}</strong>: {r['Hole_Count']} Holes<br><small>{r['Notes']}</small></div>", unsafe_allow_html=True)
            c2.button("CLR", key=f"o_{r['OOS_ID']}", on_click=complete_oos, args=(r['OOS_ID'], active_op))

        st.divider()
        if st.button("🚀 Load Daily Rhythm"):
            with get_db() as conn:
                cur = conn.cursor()
                hrs = (((total_pcs / cph) / staff_num) * 60) if total_pcs > 0 else 120
                day = now.strftime('%A')
                directives = [{"Task": "Direction Huddle", "P": "Urgent", "Z": "General", "T": 5}, {"Task": "Store Walk", "P": "High", "Z": "General", "T": 30}]
                if counts['Weather_Alert']: directives.append({"Task": "Snow/Salt", "P": "Urgent", "Z": "Outside", "T": 20})
                if day in ["Sunday", "Tuesday", "Thursday"]: directives.append({"Task": "TGP Order", "P": "Urgent", "Z": "Receiving", "T": int(hrs)})
                
                for d in directives:
                    try: cur.execute("INSERT INTO tasks VALUES (?, ?, 'Open', ?, ?, 'Unassigned', ?, ?, '', '')", (gen_id(), d["Task"], d["P"], d["Z"], d["T"], get_utc_now()))
                    except: pass
                for v in VENDOR_SCHEDULE.get(day, []):
                    try: cur.execute("INSERT INTO expected_orders VALUES (?, ?, ?, 'Pending', 'AUTO', '', '')", (gen_id(), v, day))
                    except: pass
                _internal_audit(cur, "Rhythm Loaded")
            st.rerun()

    # TICKER
    if not tk_df.empty:
        m_str = "  🛑  ".join(tk_df["Message"].tolist())
        st.markdown(f"<div class='ticker-wrap'><div class='ticker'>📢 {m_str} 🛑 </div></div>", unsafe_allow_html=True)

# --- THE 2-SECOND ENGINE ---
if should_auto_refresh:
    # This creates the high-speed fragment loop
    @st.fragment(run_every=2)
    def live_loop():
        render_full_board()
    live_loop()
else:
    render_full_board()
    st.info("💡 To enable 2-second TV mode automatically on this screen, add **?mode=tv** to the end of the URL.")
