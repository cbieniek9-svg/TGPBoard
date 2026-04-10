import streamlit as st
import pandas as pd
import sqlite3
import html
import uuid
import hashlib
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Edmonton")
except ImportError:
    import pytz
    LOCAL_TZ = pytz.timezone("America/Edmonton")

# --- DATABASE / TIME HELPERS ---
def get_utc_now(): return datetime.now(timezone.utc).isoformat()
def get_local_now(): return datetime.now(LOCAL_TZ)
def get_pin_hash(pin_str): return hashlib.sha256(str(pin_str).encode()).hexdigest()
def gen_id(): return str(uuid.uuid4().hex)

DB_FILE = "tgp_board.db"
VALID_TABLES = {"tasks", "oos", "counts", "audit", "special_orders", "expected_orders", "ticker", "staff", "settings"}

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
        
        # Initial Seed
        if conn.execute("SELECT COUNT(*) FROM counts").fetchone()[0] == 0:
            conn.execute("INSERT INTO counts VALUES (1, 0, 0, 1, ?, 0, '')", (get_utc_now(),))
        if conn.execute("SELECT COUNT(*) FROM staff").fetchone()[0] == 0:
            conn.executemany("INSERT INTO staff VALUES (?, ?)", [("Chris", 1), ("Ashley", 1), ("Luke", 1), ("Unassigned", 1)])
        if conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0] == 0:
            conn.executemany("INSERT INTO settings VALUES (?, ?)", [
                ("Cases_Per_Hour", "55"), 
                ("Admin_PIN", get_pin_hash("1234")),
                ("Global_TV_Mode", "0") # 0 = Off, 1 = On
            ])

init_db()

# --- TRANSACTIONAL LOGIC ---
def _internal_audit(cur, event):
    cur.execute("INSERT INTO audit VALUES (?, ?, ?)", (gen_id(), get_utc_now(), event))
    cur.execute("DELETE FROM audit WHERE rowid NOT IN (SELECT rowid FROM audit ORDER BY rowid DESC LIMIT 100)")

def assign_task(task_id, staff_name):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE tasks SET Assigned_To = ? WHERE Task_ID = ?", (staff_name, str(task_id)))
        _internal_audit(cur, f"Task {task_id} assigned to {staff_name}")

def handle_assign_callback(task_id, widget_key):
    new_owner = st.session_state.get(widget_key, "Unassigned")
    assign_task(task_id, new_owner)

def complete_task(task_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        conn.execute("UPDATE tasks SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE Task_ID = ?", (user, get_utc_now(), str(task_id)))
        _internal_audit(cur, f"Task {task_id} cleared by {user}")

def complete_oos(oos_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        conn.execute("UPDATE oos SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE OOS_ID = ?", (user, get_utc_now(), str(oos_id)))
        _internal_audit(cur, f"OOS {oos_id} cleared by {user}")

# --- UI CSS ---
st.markdown("""
<style>
footer { visibility: hidden; }
.stApp { background-color: #0b0f14; color: #d1d5db; }
header[data-testid="stHeader"] { background: rgba(0,0,0,0); visibility: visible; }
.header-bar { display: flex; align-items: center; border-bottom: 3px solid #38bdf8; margin-bottom: 15px; padding-bottom: 10px; }
.header-title { font-size: 28px; font-weight: 800; color: #f9fafb; flex-grow: 1; text-transform: uppercase; }
.kpi-box { background: #161b22; border-top: 4px solid #38bdf8; padding: 10px; border-radius: 4px; text-align:center; }
.data-card { background: #161b22; border-left: 5px solid #38bdf8; padding: 10px; margin-bottom: 8px; border-radius: 4px; }
.ticker-wrap { width: 100%; overflow: hidden; background-color: #facc15; padding: 10px 0; position: fixed; bottom: 0; left: 0; z-index: 999; }
.ticker { display: inline-block; white-space: nowrap; padding-left: 100%; animation: ticker 30s linear infinite; color: #000; font-family: 'Arial Black', sans-serif; font-size: 18px; }
@keyframes ticker { 0% { transform: translate3d(0, 0, 0); } 100% { transform: translate3d(-100%, 0, 0); } }
</style>
""", unsafe_allow_html=True)

# --- LOAD STATE ---
with get_db() as conn:
    c_df = pd.read_sql("SELECT * FROM counts WHERE ID = 1", conn)
    staff_df = pd.read_sql("SELECT * FROM staff", conn)
    set_df = pd.read_sql("SELECT * FROM settings", conn)
    
global_tv_active = False
if not set_df.empty:
    val = set_df.loc[set_df["Setting_Name"] == "Global_TV_Mode", "Setting_Value"]
    if not val.empty: global_tv_active = (val.iloc[0] == "1")

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("### 🔧 COMM CENTER")
    
    # MASTER REMOTE CONTROL SWITCH
    if global_tv_active:
        if st.button("🔴 STOP ALL AUTO-REFRESH", type="primary"):
            with get_db() as conn: conn.execute("UPDATE settings SET Setting_Value = '0' WHERE Setting_Name = 'Global_TV_Mode'")
            st.rerun()
    else:
        if st.button("🚀 FORCE ALL SCREENS TO TV MODE", type="secondary"):
            with get_db() as conn: conn.execute("UPDATE settings SET Setting_Value = '1' WHERE Setting_Name = 'Global_TV_Mode'")
            st.rerun()

    active_op = st.selectbox("Operator:", PREMIUM_STAFF)
    
    with st.expander("📝 Quick Actions"):
        t_desc = st.text_input("New Task")
        if st.button("Deploy") and t_desc:
            with get_db() as conn: conn.execute("INSERT INTO tasks VALUES (?, ?, 'Open', 'Routine', 'General', 'Unassigned', 15, ?, '', '')", (gen_id(), t_desc, get_utc_now()))
            st.rerun()

# --- MAIN BOARD ---
def render_board():
    with get_db() as conn:
        t_df = pd.read_sql("SELECT * FROM tasks WHERE Status = 'Open'", conn)
        oos_df = pd.read_sql("SELECT * FROM oos WHERE Status = 'Open'", conn)
        counts = pd.read_sql("SELECT * FROM counts WHERE ID = 1", conn)
        tk_df = pd.read_sql("SELECT * FROM ticker", conn)
        staff_live = pd.read_sql("SELECT * FROM staff WHERE Active = 1", conn)

    now = get_local_now()
    st.markdown(f"<div class='header-bar'><div class='header-title'>TGP CENTRE STORE // {now.strftime('%A')}</div><div style='font-size:32px; font-weight:bold;'>{now.strftime('%I:%M %p')}</div></div>", unsafe_allow_html=True)

    # KPI ROW
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Load Pcs", f"{counts['Grocery'].iloc[0] + counts['Frozen'].iloc[0]}")
    k2.metric("Staff", counts['Staff'].iloc[0])
    
    if global_tv_active:
        st.info("📡 REMOTE TV MODE ACTIVE (Auto-Updating every 3s)")

    # DATA COLUMNS
    L, R = st.columns([0.65, 0.35])
    with L:
        st.markdown("#### ACTIVE TASKS")
        for _, r in t_df.iterrows():
            c1, c2 = st.columns([0.8, 0.2])
            c1.markdown(f"<div class='data-card'><strong>{r['Task_Detail']}</strong><br><small>ZONE: {r['Zone']}</small></div>", unsafe_allow_html=True)
            c2.button("DONE", key=f"t_{r['Task_ID']}", on_click=complete_task, args=(r['Task_ID'], active_op))

    with R:
        st.markdown("#### SHELF HOLES")
        for _, r in oos_df.iterrows():
            st.markdown(f"<div class='data-card' style='border-left-color:#ef4444;'><strong>{r['Zone']}</strong>: {r['Hole_Count']} Holes</div>", unsafe_allow_html=True)
            st.button("CLR", key=f"o_{r['OOS_ID']}", on_click=complete_oos, args=(r['OOS_ID'], active_op))

    # TICKER
    if not tk_df.empty:
        msg = "  🛑  ".join(tk_df["Message"].tolist())
        st.markdown(f"<div class='ticker-wrap'><div class='ticker'>📢 {msg} 🛑 </div></div>", unsafe_allow_html=True)

# --- THE ENGINE ---
# If Global TV Mode is ON in the database, this screen starts updating immediately.
if global_tv_active:
    @st.fragment(run_every=3)
    def live_loop(): render_board()
    live_loop()
else:
    render_board()
    st.warning("⚠️ Manual Refresh Only. Use the 'Force TV Mode' button in the menu to start auto-updating.")
