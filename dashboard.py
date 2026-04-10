import streamlit as st
import pandas as pd
import sqlite3
import html
import uuid
import hashlib
from datetime import datetime, timezone, timedelta

# --- HARDENED: STRICT TIMEZONE HANDLING ---
try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Edmonton")
except ImportError:
    import pytz
    LOCAL_TZ = pytz.timezone("America/Edmonton")

def get_utc_now(): return datetime.now(timezone.utc).isoformat()
def get_local_now(): return datetime.now(LOCAL_TZ)
def get_pin_hash(pin_str): return hashlib.sha256(str(pin_str).encode()).hexdigest()
def gen_id(): return str(uuid.uuid4().hex)

DB_FILE = "tgp_board.db"

# --- BOARD CONFIGURATION ---
st.set_page_config(page_title="TGP Comm Board", layout="wide", initial_sidebar_state="collapsed")

# URL QUERY PARAMETER CHECK (For permanently locking a TV into refresh mode)
query_params = st.query_params
is_tv_url_mode = str(query_params.get("tvmode", "")).lower() in ["true", "1", "yes"] or str(query_params.get("mode", "")).lower() == "tv"

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
aisles = ["Aisle 1", "Aisle 2", "Aisle 3", "Aisle 4", "Aisle 5", "Aisle 6", "Aisle 7", "Aisle 8", "Receiving", "Freezer", "Bakery", "Outside"]

# --- SQLITE HIGH-PERFORMANCE CONNECTION ---
def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=15.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# --- DATABASE INITIALIZATION ---
def init_db():
    with get_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS staff
                        (Name TEXT PRIMARY KEY, Active INTEGER)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS tasks
                        (Task_ID TEXT PRIMARY KEY, Task_Detail TEXT, Status TEXT, Priority TEXT, 
                         Zone TEXT, Assigned_To TEXT REFERENCES staff(Name) ON DELETE RESTRICT, 
                         Est_Mins INTEGER, Time_Submitted TEXT, Closed_By TEXT, Time_Closed TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS oos
                        (OOS_ID TEXT PRIMARY KEY, Zone TEXT, Hole_Count INTEGER, Notes TEXT, 
                         Status TEXT, Logged_By TEXT, Time_Logged TEXT, Closed_By TEXT, Time_Closed TEXT)''')
        
        try: conn.execute("SELECT ID FROM counts")
        except sqlite3.OperationalError: conn.execute("DROP TABLE IF EXISTS counts")
        
        conn.execute('''CREATE TABLE IF NOT EXISTS counts
                        (ID INTEGER PRIMARY KEY CHECK (ID = 1), Grocery INTEGER, Frozen INTEGER, 
                         Staff INTEGER CHECK (Staff >= 1 AND Staff <= 100), Last_Update TEXT, 
                         Weather_Alert INTEGER, Ticker_Msg TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS audit
                        (Log_ID TEXT PRIMARY KEY, Timestamp TEXT, Event TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS special_orders
                        (Order_ID TEXT PRIMARY KEY, Customer TEXT, Item TEXT, Contact TEXT, 
                         Location TEXT, Status TEXT, Logged_By TEXT, Time_Logged TEXT, 
                         Closed_By TEXT, Time_Closed TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS expected_orders
                        (Exp_ID TEXT PRIMARY KEY, Vendor TEXT, Expected_Day TEXT, Status TEXT, 
                         Logged_By TEXT, Closed_By TEXT, Time_Closed TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS ticker
                        (Msg_ID TEXT PRIMARY KEY, Message TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS settings
                        (Setting_Name TEXT PRIMARY KEY, Setting_Value TEXT)''')

        # Performance Indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(Status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_oos_status ON oos(Status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON special_orders(Status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_expected_status ON expected_orders(Status)")
        
        # Constraints to prevent duplicate Daily Rhythm loads
        try: conn.execute("CREATE UNIQUE INDEX idx_tasks_unique_open ON tasks(Task_Detail) WHERE Status = 'Open'")
        except sqlite3.OperationalError: pass
        try: conn.execute("CREATE UNIQUE INDEX idx_exp_unique_pending ON expected_orders(Vendor) WHERE Status = 'Pending'")
        except sqlite3.OperationalError: pass

        # Seed Defaults
        if conn.execute("SELECT COUNT(*) FROM counts").fetchone()[0] == 0:
            conn.execute("INSERT INTO counts VALUES (1, 0, 0, 1, ?, 0, '')", (get_utc_now(),))
        if conn.execute("SELECT COUNT(*) FROM staff").fetchone()[0] == 0:
            conn.executemany("INSERT INTO staff VALUES (?, ?)", [("John", 1), ("Sarah", 1), ("Mike", 1), ("Emily", 1)])
            conn.execute("INSERT INTO staff VALUES ('Unassigned', 1)") 
        if conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0] == 0:
            conn.executemany("INSERT INTO settings VALUES (?, ?)", [
                ("Cases_Per_Hour", "55"), 
                ("Admin_PIN", get_pin_hash("1234")),
                ("Global_TV_Mode", "0")
            ])

init_db()

# --- INTERNAL SERVICE LAYER (ATOMIC TRANSACTIONS) ---
def _internal_audit(cur, event):
    cur.execute("INSERT INTO audit VALUES (?, ?, ?)", (gen_id(), get_utc_now(), event))
    cur.execute("DELETE FROM audit WHERE rowid NOT IN (SELECT rowid FROM audit ORDER BY rowid DESC LIMIT 100)")

def _strict_update(cur, query, params):
    cur.execute(query, params)
    if cur.rowcount != 1: raise ValueError("Transaction failed: Target record not found or duplicate.")

def assign_task(task_id, staff_name):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE tasks SET Assigned_To = ? WHERE Task_ID = ?", (staff_name, str(task_id)))
        _internal_audit(cur, f"Task {task_id} assigned to {staff_name}")

def handle_assign_callback(task_id, widget_key):
    new_owner = st.session_state.get(widget_key, "Unassigned")
    try: assign_task(task_id, new_owner)
    except Exception as e: st.error(f"Assignment failed: {e}")

def complete_task(task_id, premium_user):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT Assigned_To FROM tasks WHERE Task_ID = ?", (str(task_id),))
        row = cur.fetchone()
        if not row: raise ValueError(f"Task {task_id} not found.")
        worker = row[0] if row[0] not in ["", "Unassigned", None] else "the Team"
        _strict_update(cur, "UPDATE tasks SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE Task_ID = ?", 
                     (premium_user, get_utc_now(), str(task_id)))
        _internal_audit(cur, f"Task {task_id} completed by {worker} (Verified by {premium_user})")

def complete_oos(oos_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE oos SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE OOS_ID = ?", 
                     (user, get_utc_now(), str(oos_id)))
        _internal_audit(cur, f"OOS {oos_id} cleared by {user}")

def complete_special_order(order_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE special_orders SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE Order_ID = ?", 
                     (user, get_utc_now(), str(order_id)))
        _internal_audit(cur, f"Special Order {order_id} cleared by {user}")

def complete_expected_order(exp_id, user):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "UPDATE expected_orders SET Status = 'Closed', Closed_By = ?, Time_Closed = ? WHERE Exp_ID = ?", 
                     (user, get_utc_now(), str(exp_id)))
        _internal_audit(cur, f"Expected Inbound {exp_id} received by {user}")

def delete_ticker(msg_id):
    with get_db() as conn:
        cur = conn.cursor()
        _strict_update(cur, "DELETE FROM ticker WHERE Msg_ID = ?", (str(msg_id),))
        _internal_audit(cur, "Broadcast message cleared")

def execute_eod_reset():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM tasks WHERE Status = 'Closed'")
        cur.execute("DELETE FROM special_orders WHERE Status = 'Closed'")
        cur.execute("DELETE FROM expected_orders WHERE Status != 'Pending'")
        cur.execute("DELETE FROM oos")
        cur.execute("DELETE FROM ticker")
        _internal_audit(cur, "EOD RESET COMPLETED by Admin")

# --- UI STYLING (HARDENED FOR TV MODE) ---
st.markdown(f"""
<style>
footer {{ visibility: hidden; }}
#MainMenu {{ visibility: hidden; }}
.stApp {{ background-color: #0b0f14; color: #d1d5db; }}

/* HEADER TOGGLE FIX: Keep arrow visible but hide background */
header[data-testid="stHeader"] {{ background: rgba(0,0,0,0); visibility: visible; }}
header[data-testid="stHeader"] > div:first-child {{ visibility: hidden; }}

.block-container {{ padding-top: 2rem; padding-bottom: 5rem; padding-left: 2rem; padding-right: 2rem; max-width: 100%; }}

.header-bar {{ display: flex; align-items: center; border-bottom: 3px solid #38bdf8; margin-bottom: 15px; padding-bottom: 10px; }}
.header-title {{ font-size: 28px; font-weight: 800; color: #f9fafb; flex-grow: 1; text-transform: uppercase; letter-spacing: 1px;}}
.kpi-container {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; margin-bottom: 20px; }}
.kpi-box {{ background: #161b22; border-top: 4px solid #38bdf8; padding: 12px; border-radius: 4px; text-align:center; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }}
.kpi-box.urgent {{ border-top-color: #ef4444; }}
.kpi-label {{ font-size: 11px; font-weight: 700; color: #8b949e; text-transform: uppercase; }}
.kpi-value {{ font-size: 22px; font-weight: 800; color: #f9fafb; }}
.data-card {{ background: #161b22; border-left: 5px solid #38bdf8; padding: 10px 15px; margin-bottom: 8px; border-radius: 4px; }}
.data-urgent {{ border-left-color: #ef4444; background: rgba(239, 68, 68, 0.05); }}
.sect-header {{ font-size: 16px; font-weight: 700; color: #38bdf8; border-bottom: 1px solid #30363d; padding-bottom: 5px; margin: 15px 0 10px 0; text-transform: uppercase; }}
.ticker-wrap {{ width: 100%; overflow: hidden; background-color: #facc15; border-top: 2px solid #ca8a04; border-bottom: 2px solid #ca8a04; padding: 12px 0; position: fixed; bottom: 0; left: 0; z-index: 999; }}
.ticker {{ display: inline-block; white-space: nowrap; padding-left: 100%; animation: ticker 30s linear infinite; color: #000; font-family: 'Arial Black', sans-serif; font-size: 18px; text-transform: uppercase; }}
@keyframes ticker {{ 0% {{ transform: translate3d(0, 0, 0); }} 100% {{ transform: translate3d(-100%, 0, 0); }} }}
div[data-testid="stButton"] > button {{ border-radius: 4px; border: 1px solid #30363d; background: #21262d; color: #c9d1d9; font-weight: 700; width: 100%; }}
div[data-testid="stButton"] > button:hover {{ border-color: #38bdf8; color: #38bdf8; }}
</style>
""", unsafe_allow_html=True)

# --- LOAD STATE FOR SIDEBAR ---
with get_db() as conn:
    c_df = pd.read_sql("SELECT * FROM counts WHERE ID = 1", conn)
    staff_df = pd.read_sql("SELECT * FROM staff", conn)
    set_df = pd.read_sql("SELECT * FROM settings", conn)

now_local = get_local_now()
day_name = now_local.strftime("%A")

g_pcs = int(c_df["Grocery"].iloc[0]) if not c_df.empty else 0
f_pcs = int(c_df["Frozen"].iloc[0]) if not c_df.empty else 0
staff_count = max(1, int(c_df["Staff"].iloc[0])) if not c_df.empty else 1

master_staff = staff_df[staff_df["Name"] != "Unassigned"]["Name"].tolist() if not staff_df.empty else []
active_staff = staff_df[(staff_df["Active"] == 1) & (staff_df["Name"] != "Unassigned")]["Name"].tolist() if not staff_df.empty else []

admin_pin_hash = get_pin_hash("1234") # Default fallback if DB is blank
global_tv_active = False
cases_per_hour = 55.0

if not set_df.empty:
    pin_val = set_df.loc[set_df["Setting_Name"] == "Admin_PIN", "Setting_Value"]
    if not pin_val.empty: admin_pin_hash = pin_val.iloc[0]
    
    tv_val = set_df.loc[set_df["Setting_Name"] == "Global_TV_Mode", "Setting_Value"]
    if not tv_val.empty: global_tv_active = (tv_val.iloc[0] == "1")
    
    cph_val = set_df.loc[set_df["Setting_Name"] == "Cases_Per_Hour", "Setting_Value"]
    if not cph_val.empty: cases_per_hour = float(cph_val.iloc[0])

# Determine if the 2-second refresh loop should run
should_auto_refresh = is_tv_url_mode or global_tv_active or st.session_state.get("tv_toggle", False)

# --- FULL SIDEBAR OPERATIONAL CONTROLS ---
with st.sidebar:
    st.markdown("### 🔧 COMM CENTER")
    
    # Local Screen Toggle
    st.toggle("📺 Local TV Display Mode", key="tv_toggle")
    
    # Global Master Switch (Controls the floor TV from your phone)
    if global_tv_active:
        if st.button("🔴 STOP ALL AUTO-REFRESH", type="primary"):
            with get_db() as conn: conn.execute("UPDATE settings SET Setting_Value = '0' WHERE Setting_Name = 'Global_TV_Mode'")
            st.rerun()
    else:
        if st.button("🚀 FORCE ALL SCREENS TO TV MODE", type="secondary"):
            with get_db() as conn: conn.execute("UPDATE settings SET Setting_Value = '1' WHERE Setting_Name = 'Global_TV_Mode'")
            st.rerun()

    active_op = st.selectbox("Premium Operator:", PREMIUM_STAFF)
    
    if not should_auto_refresh:
        if st.button("🔄 Sync Board Now"): st.rerun()

    with st.expander("👥 Shift Roster Settings"):
        st.caption("Select floor staff working today.")
        selected_active = st.multiselect("Active Today:", master_staff, default=active_staff)
        if selected_active != active_staff:
            with get_db() as conn:
                conn.execute("UPDATE staff SET Active = 0 WHERE Name != 'Unassigned'")
                if selected_active:
                    placeholders = ','.join(['?'] * len(selected_active))
                    conn.execute(f"UPDATE staff SET Active = 1 WHERE Name IN ({placeholders})", selected_active)
            st.rerun()
            
        st.divider()
        new_staff = st.text_input("Add New Floor Staff")
        if st.button("Add to Roster") and new_staff and new_staff.strip() not in master_staff:
            with get_db() as conn:
                conn.execute("INSERT OR IGNORE INTO staff (Name, Active) VALUES (?, 1)", (html.escape(new_staff.strip()),))
            st.rerun()

    st.divider()
    st.markdown("**1. Broadcast Manager**")
    with st.form("ticker_form", clear_on_submit=True):
        new_msg = st.text_input("Add Ticker Message")
        if st.form_submit_button("Broadcast") and new_msg.strip():
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO ticker VALUES (?, ?)", (gen_id(), html.escape(new_msg.strip())))
                _internal_audit(cur, f"Broadcast added: {new_msg.strip()}")
            st.rerun()

    st.divider()
    st.markdown("**2. Deploy Task**")
    with st.form("task_form", clear_on_submit=True):
        t_desc = st.text_input("Task Description")
        t_zone = st.selectbox("Zone", aisles)
        t_pri = st.selectbox("Priority", ["Routine", "High", "Urgent"])
        t_est = st.number_input("Est. Time (Mins)", min_value=1, value=15, step=5)
        if st.form_submit_button("Deploy Task") and t_desc.strip():
            with get_db() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("INSERT INTO tasks VALUES (?, ?, 'Open', ?, ?, 'Unassigned', ?, ?, '', '')", 
                                 (gen_id(), html.escape(t_desc.strip().capitalize()), t_pri, t_zone, t_est, get_utc_now()))
                    _internal_audit(cur, f"Task Deployed: {t_desc.strip()}")
                except sqlite3.IntegrityError:
                    st.error("Task already exists and is Open.")
            st.rerun()

    st.divider()
    st.markdown("**3. Load / Labor Engine**")
    with st.form("load_form"):
        in_g = st.number_input("Grocery Pcs", min_value=0, value=g_pcs)
        in_f = st.number_input("Frozen Pcs", min_value=0, value=f_pcs)
        in_s = st.number_input("Active Staff", min_value=1, value=staff_count)
        if st.form_submit_button("Calculate Labor"):
            with get_db() as conn:
                conn.execute("UPDATE counts SET Grocery=?, Frozen=?, Staff=?, Last_Update=? WHERE ID = 1", 
                             (in_g, in_f, in_s, get_utc_now()))
            st.rerun()

    st.divider()
    st.markdown("**4. Log OOS Holes**")
    with st.form("oos_form", clear_on_submit=True):
        o_z = st.selectbox("Aisle", aisles[:8] + ["Freezer", "Bakery"])
        o_c = st.number_input("Number of Holes", min_value=1, value=1)
        o_n = st.text_input("Notes (e.g., Deletes, Missing DSD)")
        if st.form_submit_button("Log Holes"):
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO oos VALUES (?, ?, ?, ?, 'Open', ?, ?, '', '')", 
                             (gen_id(), o_z, o_c, html.escape(o_n.strip()), active_op, get_utc_now()))
                _internal_audit(cur, f"Logged {o_c} holes in {o_z}")
            st.rerun()

    st.divider()
    st.markdown("**5. Log Customer Order**")
    with st.form("order_form", clear_on_submit=True):
        c_loc = st.selectbox("Location Ordered Under", ORDER_LOCATIONS)
        c_item = st.text_input("Item")
        c_name = st.text_input("Customer Name")
        if st.form_submit_button("Log Order") and c_item.strip() and c_name.strip():
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO special_orders VALUES (?, ?, ?, '', ?, 'Open', ?, ?, '', '')", 
                             (gen_id(), html.escape(c_name.strip()), html.escape(c_item.strip()), c_loc, active_op, get_utc_now()))
                _internal_audit(cur, f"Customer Order Logged for Location {c_loc}")
            st.rerun()

    st.divider()
    st.markdown("**6. Add Expected Vendor**")
    with st.form("vendor_form", clear_on_submit=True):
        e_ven = st.text_input("Vendor (e.g. Direct Plus, Saputo)")
        if st.form_submit_button("Log Inbound") and e_ven.strip():
            with get_db() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("INSERT INTO expected_orders VALUES (?, ?, ?, 'Pending', ?, '', '')", 
                                 (gen_id(), html.escape(e_ven.strip()), day_name, active_op))
                    _internal_audit(cur, f"Extra Inbound Logged: {e_ven.strip()}")
                except sqlite3.IntegrityError:
                    st.error("Vendor already expected today.")
            st.rerun()

    st.divider()
    if st.button("🌦️ Toggle Weather Alert"):
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE counts SET Weather_Alert = CASE WHEN Weather_Alert = 1 THEN 0 ELSE 1 END WHERE ID = 1")
            _internal_audit(cur, "Weather alert toggled")
        st.rerun()

    # --- ADMIN CONSOLE ---
    st.divider()
    with st.expander("🛡️ Admin Console"):
        pin = st.text_input("Admin PIN", type="password")
        if pin and admin_pin_hash and get_pin_hash(pin) == admin_pin_hash:
            st.success("Admin Unlocked")
            
            st.markdown("**System Settings**")
            with st.form("metric_form"):
  
