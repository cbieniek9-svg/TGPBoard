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

# --- STRICT ISO-8601 UTC TIMESTAMPS ---
def get_utc_now(): return datetime.now(timezone.utc).isoformat()
def get_local_now(): return datetime.now(LOCAL_TZ)
def get_pin_hash(pin_str): return hashlib.sha256(str(pin_str).encode()).hexdigest()
def gen_id(): return str(uuid.uuid4().hex)

DB_FILE = "tgp_board.db"
VALID_TABLES = {"tasks", "oos", "counts", "audit", "special_orders", "expected_orders", "ticker", "staff", "settings"}

# --- BOARD CONFIGURATION ---
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

# --- SQLITE HIGH-PERFORMANCE CONNECTION ---
def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=15.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# --- DATABASE INITIALIZATION, INDEXING & FK CONSTRAINTS ---
def init_db():
    with get_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS staff
                        (Name TEXT PRIMARY KEY, Active INTEGER)''')
                        
        # Changed to RESTRICT to align with UI expectations of blocking deletion
        conn.execute('''CREATE TABLE IF NOT EXISTS tasks
                        (Task_ID TEXT PRIMARY KEY, Task_Detail TEXT, Status TEXT, Priority TEXT, 
                         Zone TEXT, Assigned_To TEXT REFERENCES staff(Name) ON DELETE RESTRICT, 
                         Est_Mins INTEGER, Time_Submitted TEXT, Closed_By TEXT, Time_Closed TEXT)''')
                         
        conn.execute('''CREATE TABLE IF NOT EXISTS oos
                        (OOS_ID TEXT PRIMARY KEY, Zone TEXT, Hole_Count INTEGER, Notes TEXT, 
                         Status TEXT, Logged_By TEXT, Time_Logged TEXT, Closed_By TEXT, Time_Closed TEXT)''')
        
        try: conn.execute("SELECT ID FROM counts")
        except sqlite3.OperationalError: conn.execute("DROP TABLE IF EXISTS counts")
            
        # Added CHECK constraint to prevent absurd staff numbers causing UI math errors
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_rowid ON audit(rowid)")
        
        try: conn.execute("CREATE UNIQUE INDEX idx_tasks_unique_open ON tasks(Task_Detail) WHERE Status = 'Open'")
        except sqlite3.OperationalError: pass
        try: conn.execute("CREATE UNIQUE INDEX idx_exp_unique_pending ON expected_orders(Vendor) WHERE Status = 'Pending'")
        except sqlite3.OperationalError: pass

        if conn.execute("SELECT COUNT(*) FROM counts").fetchone()[0] == 0:
            conn.execute("INSERT INTO counts VALUES (1, 0, 0, 1, ?, 0, '')", (get_utc_now(),))
        if conn.execute("SELECT COUNT(*) FROM staff").fetchone()[0] == 0:
            conn.executemany("INSERT INTO staff VALUES (?, ?)", [("John", 1), ("Sarah", 1), ("Mike", 1), ("Emily", 1)])
            conn.execute("INSERT INTO staff VALUES ('Unassigned', 1)") 
        if conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0] == 0:
            conn.executemany("INSERT INTO settings VALUES (?, ?)", [("Cases_Per_Hour", "55"), ("Admin_PIN", get_pin_hash("1234"))])

init_db()

# --- INTERNAL SERVICE LAYER (BUSINESS LOGIC & TRANSACTIONS) ---
def _internal_audit(cur, event):
    cur.execute("INSERT INTO audit VALUES (?, ?, ?)", (gen_id(), get_utc_now(), event))
    cur.execute("DELETE FROM audit WHERE rowid NOT IN (SELECT rowid FROM audit ORDER BY rowid DESC LIMIT 100)")

def _strict_update(cur, query, params):
    cur.execute(query, params)
    if cur.rowcount != 1:
        raise ValueError("Transaction failed: Target record not found or duplicate.")

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


# --- UI STYLING & RESPONSIVE DESIGN ---
st.markdown(f"""
<style>
footer {{ visibility: hidden; }}
#MainMenu {{ visibility: hidden; }}
.stApp {{ background-color: #0b0f14; color: #d1d5db; }}
header {{ visibility: hidden; }}
.block-container {{ padding-top: 1rem; padding-bottom: 5rem; padding-left: 2rem; padding-right: 2rem; max-width: 100%; }}
@media screen and (max-width: 1024px) {{
    header {{ visibility: visible; background-color: #0b0f14; }}
    .block-container {{ padding-top: 4rem; padding-left: 1rem; padding-right: 1rem; }}
}}
.header-bar {{ display: flex; align-items: center; border-bottom: 3px solid #38bdf8; margin-bottom: 15px; padding-bottom: 10px; }}
.header-title {{ font-size: 28px; font-weight: 800; color: #f9fafb; flex-grow: 1; text-transform: uppercase; letter-spacing: 1px;}}
.kpi-container {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; margin-bottom: 20px; }}
.kpi-box {{ background: #161b22; border-top: 4px solid #38bdf8; padding: 12px; border-radius: 4px; text-align:center; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }}
.kpi-box.urgent {{ border-top-color: #ef4444; }}
.kpi-label {{ font-size: 11px; font-weight: 700; color: #8b949e; text-transform: uppercase; }}
.kpi-value {{ font-size: 22px; font-weight: 800; color: #f9fafb; }}
.data-card {{ background: #161b22; border-left: 5px solid #38bdf8; padding: 10px 15px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; border-radius: 4px; }}
.data-urgent {{ border-left-color: #ef4444; background: rgba(239, 68, 68, 0.05); }}
.sect-header {{ font-size: 16px; font-weight: 700; color: #38bdf8; border-bottom: 1px solid #30363d; padding-bottom: 5px; margin: 15px 0 10px 0; text-transform: uppercase; }}
.assign-text {{ font-size: 12px; color: #8b949e; font-weight: bold; }}
.ticker-wrap {{ width: 100%; overflow: hidden; background-color: #facc15; border-top: 2px solid #ca8a04; border-bottom: 2px solid #ca8a04; padding: 12px 0; position: fixed; bottom: 0; left: 0; z-index: 999; box-shadow: 0 -4px 10px rgba(0,0,0,0.5); }}
.ticker {{ display: inline-block; white-space: nowrap; padding-left: 100%; animation: ticker 30s linear infinite; color: #000; font-family: 'Arial Black', sans-serif; font-size: 18px; letter-spacing: 2px; text-transform: uppercase; }}
@keyframes ticker {{ 0% {{ transform: translate3d(0, 0, 0); }} 100% {{ transform: translate3d(-100%, 0, 0); }} }}
div[data-testid="stButton"] > button {{ border-radius: 4px; border: 1px solid #30363d; background: #21262d; color: #c9d1d9; font-weight: 700; width: 100%; }}
div[data-testid="stButton"] > button:hover {{ border-color: #38bdf8; color: #38bdf8; }}
</style>
""", unsafe_allow_html=True)


# --- DYNAMIC SIDEBAR LOAD ---
with get_db() as conn:
    c_df = pd.read_sql("SELECT * FROM counts WHERE ID = 1", conn)
    staff_df = pd.read_sql("SELECT * FROM staff", conn)
    set_df = pd.read_sql("SELECT * FROM settings", conn)

now_local = get_local_now()
day_name = now_local.strftime("%A")
aisles = ["Aisle 1", "Aisle 2", "Aisle 3", "Aisle 4", "Aisle 5", "Aisle 6", "Aisle 7", "Aisle 8", "Receiving", "Freezer", "Bakery", "Outside"]

g_pcs = int(c_df["Grocery"].iloc[0]) if not c_df.empty else 0
f_pcs = int(c_df["Frozen"].iloc[0]) if not c_df.empty else 0
staff_count = max(1, int(c_df["Staff"].iloc[0])) if not c_df.empty else 1

master_staff = staff_df[staff_df["Name"] != "Unassigned"]["Name"].tolist() if not staff_df.empty else []
active_staff = staff_df[(staff_df["Active"] == 1) & (staff_df["Name"] != "Unassigned")]["Name"].tolist() if not staff_df.empty else []

admin_pin_hash = ""
if not set_df.empty:
    pin_val = set_df.loc[set_df["Setting_Name"] == "Admin_PIN", "Setting_Value"]
    if not pin_val.empty: admin_pin_hash = pin_val.iloc[0]


# --- SIDEBAR: OPERATIONAL CONTROLS ---
with st.sidebar:
    st.markdown("### 🔧 COMM CENTER")
    
    is_tv_mode = st.toggle("📺 TV Display Mode (Auto-Refreshes)")
    active_op = st.selectbox("Premium Operator:", PREMIUM_STAFF)
    
    if not is_tv_mode:
        if st.button("🔄 Sync Board Now"): st.rerun()

    with st.expander("👥 Shift Roster Settings (Floor Staff)"):
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
                conn.execute("INSERT OR IGNORE INTO staff (Name, Active) VALUES (?, 1)", (new_staff.strip(),))
            st.rerun()

    st.divider()
    st.markdown("**1. Broadcast Manager**")
    with st.form("ticker_add", clear_on_submit=True):
        new_msg = st.text_input("Add Ticker Message")
        if st.form_submit_button("Broadcast") and new_msg.strip():
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO ticker VALUES (?, ?)", (gen_id(), new_msg.strip()))
                _internal_audit(cur, f"Broadcast added: {new_msg.strip()}")
            st.rerun()

    st.divider()
    st.markdown("**2. Deploy Task**")
    with st.form("custom_task_form", clear_on_submit=True):
        t_desc = st.text_input("Task Description")
        t_zone = st.selectbox("Zone", aisles)
        t_pri = st.selectbox("Priority", ["Routine", "High", "Urgent"])
        t_est = st.number_input("Est. Time (Mins)", min_value=1, value=15, step=5)
        if st.form_submit_button("Deploy Task") and t_desc.strip():
            with get_db() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("INSERT INTO tasks VALUES (?, ?, 'Open', ?, ?, 'Unassigned', ?, ?, '', '')", 
                                 (gen_id(), t_desc.strip().capitalize(), t_pri, t_zone, t_est, get_utc_now()))
                    _internal_audit(cur, f"Task Deployed: {t_desc.strip()}")
                except sqlite3.IntegrityError:
                    st.error("Task already exists and is Open.")
            st.rerun()

    st.divider()
    st.markdown("**3. Load / Labor Engine**")
    with st.form("load_form"):
        in_g_pcs = st.number_input("Grocery Pcs", min_value=0, value=g_pcs)
        in_f_pcs = st.number_input("Frozen Pcs", min_value=0, value=f_pcs)
        in_staff = st.number_input("Active Staff", min_value=1, value=staff_count)
        if st.form_submit_button("Calculate Labor"):
            with get_db() as conn:
                conn.execute("UPDATE counts SET Grocery=?, Frozen=?, Staff=?, Last_Update=? WHERE ID = 1", 
                             (in_g_pcs, in_f_pcs, in_staff, get_utc_now()))
            st.rerun()

    st.divider()
    st.markdown("**4. Log OOS Holes**")
    with st.form("oos_form", clear_on_submit=True):
        oos_zone = st.selectbox("Aisle", aisles[:8] + ["Freezer", "Bakery"])
        oos_count = st.number_input("Number of Holes", min_value=1, value=1)
        oos_notes = st.text_input("Notes (e.g., Deletes, Missing DSD)")
        if st.form_submit_button("Log Holes"):
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO oos VALUES (?, ?, ?, ?, 'Open', ?, ?, '', '')", 
                             (gen_id(), oos_zone, oos_count, oos_notes.strip(), active_op, get_utc_now()))
                _internal_audit(cur, f"Logged {oos_count} holes in {oos_zone}")
            st.rerun()

    st.divider()
    st.markdown("**5. Log Customer Order**")
    with st.form("cust_order_form", clear_on_submit=True):
        c_loc = st.selectbox("Location Ordered Under", ORDER_LOCATIONS)
        c_item = st.text_input("Item")
        c_name = st.text_input("Customer Name")
        if st.form_submit_button("Log Order") and c_item.strip() and c_name.strip():
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO special_orders VALUES (?, ?, ?, '', ?, 'Open', ?, ?, '', '')", 
                             (gen_id(), c_name.strip(), c_item.strip(), c_loc, active_op, get_utc_now()))
                _internal_audit(cur, f"Customer Order Logged for Location {c_loc}")
            st.rerun()

    st.divider()
    st.markdown("**6. Add Extra Inbound Vendor**")
    with st.form("exp_order_form", clear_on_submit=True):
        e_ven = st.text_input("Vendor (e.g. Direct Plus, Saputo)")
        if st.form_submit_button("Log Extra Inbound") and e_ven.strip():
            with get_db() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("INSERT INTO expected_orders VALUES (?, ?, ?, 'Pending', ?, '', '')", 
                                 (gen_id(), e_ven.strip(), day_name, active_op))
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
                current_cph = 55.0
                if not set_df.empty:
                    cph_val = set_df.loc[set_df["Setting_Name"] == "Cases_Per_Hour", "Setting_Value"]
                    if not cph_val.empty: current_cph = float(cph_val.iloc[0])
                
                new_cph = st.number_input("Target Cases Per Hour", value=current_cph)
                new_pin = st.text_input("Change Admin PIN (Leave blank to keep current)", type="password")
                if st.form_submit_button("Update Settings"):
                    with get_db() as conn:
                        conn.execute("UPDATE settings SET Setting_Value = ? WHERE Setting_Name = 'Cases_Per_Hour'", (str(new_cph),))
                        if new_pin.strip():
                            conn.execute("UPDATE settings SET Setting_Value = ? WHERE Setting_Name = 'Admin_PIN'", (get_pin_hash(new_pin.strip()),))
                    st.rerun()
            
            st.markdown("**Roster Management**")
            with st.form("del_staff_form"):
                if master_staff:
                    del_staff = st.selectbox("Permanently Delete Floor Staff", master_staff)
                    if st.form_submit_button("Delete"):
                        try:
                            with get_db() as conn:
                                cur = conn.cursor()
                                _strict_update(cur, "DELETE FROM staff WHERE Name = ?", (del_staff,))
                            st.rerun()
                        except sqlite3.IntegrityError:
                            st.error(f"Cannot delete {del_staff}. They have active tasks assigned. Reassign them first.")
                else:
                    st.caption("No staff to delete.")
                    st.form_submit_button("Delete", disabled=True)

            st.markdown("**Database Reset**")
            with st.form("eod_form"):
                st.caption("⚠️ Purges closed tasks, holes, completed orders, and tickers.")
                if st.form_submit_button("🌙 EXECUTE EOD RESET", type="primary"):
                    execute_eod_reset()
                    st.rerun()
        elif pin:
            st.error("Invalid PIN")


# --- CORE RENDERING FUNCTION ---
def render_main_board():
    with get_db() as conn:
        t_df = pd.read_sql("SELECT * FROM tasks", conn)
        oos_df = pd.read_sql("SELECT * FROM oos", conn)
        s_df = pd.read_sql("SELECT * FROM special_orders", conn)
        e_df = pd.read_sql("SELECT * FROM expected_orders", conn)
        curr_c_df = pd.read_sql("SELECT * FROM counts WHERE ID = 1", conn)
        curr_staff_df = pd.read_sql("SELECT * FROM staff", conn)
        tk_df = pd.read_sql("SELECT * FROM ticker", conn)
        live_set_df = pd.read_sql("SELECT * FROM settings", conn)

    curr_now = get_local_now()
    curr_day = curr_now.strftime("%A")

    st.markdown(f"<div class='header-bar'><div class='header-title'>TGP CENTRE STORE // {curr_day}</div><div style='color:#8b949e; font-size: 32px; font-weight: bold;'>{curr_now.strftime('%I:%M %p')}</div></div>", unsafe_allow_html=True)

    curr_g_pcs = int(curr_c_df["Grocery"].iloc[0]) if not curr_c_df.empty else 0
    curr_f_pcs = int(curr_c_df["Frozen"].iloc[0]) if not curr_c_df.empty else 0
    curr_staff = max(1, int(curr_c_df["Staff"].iloc[0])) if not curr_c_df.empty else 1
    curr_weather = bool(curr_c_df["Weather_Alert"].iloc[0]) if not curr_c_df.empty else False
    
    live_cph = 55.0
    if not live_set_df.empty:
        cph_val = live_set_df.loc[live_set_df["Setting_Name"] == "Cases_Per_Hour", "Setting_Value"]
        if not cph_val.empty: live_cph = max(1.0, float(cph_val.iloc[0])) # Absolute floor prevents div-by-zero
    
    live_active_staff = curr_staff_df[(curr_staff_df["Active"] == 1) & (curr_staff_df["Name"] != "Unassigned")]["Name"].tolist() if not curr_staff_df.empty else []

    total_pcs = curr_g_pcs + curr_f_pcs
    freight_hours = total_pcs / live_cph

    open_t = t_df[t_df["Status"] == "Open"].copy() if not t_df.empty and "Status" in t_df.columns else pd.DataFrame()
    task_mins = pd.to_numeric(open_t["Est_Mins"], errors='coerce').fillna(15).sum() if not open_t.empty else 0
    task_hours = task_mins / 60.0

    total_hours_needed = (freight_hours + task_hours) / curr_staff
    completion_time = (curr_now + timedelta(hours=total_hours_needed)).strftime('%I:%M %p') if (total_pcs > 0 or task_mins > 0) else "N/A"

    st.markdown(f"""
    <div class='kpi-container'>
        <div class='kpi-box'><div class='kpi-label'>Load Total</div><div class='kpi-value'>{total_pcs} Pcs</div></div>
        <div class='kpi-box'><div class='kpi-label'>Active Staff</div><div class='kpi-value'>{curr_staff}</div></div>
        <div class='kpi-box'><div class='kpi-label'>Task Workload</div><div class='kpi-value'>{int(task_mins)} Mins</div></div>
        <div class='kpi-box {'urgent' if total_hours_needed > 7.5 else ''}'><div class='kpi-label'>Time to Complete</div><div class='kpi-value'>{round(total_hours_needed,1)} Hrs</div></div>
        <div class='kpi-box'><div class='kpi-label'>True ETA</div><div class='kpi-value' style='color:#00e676;'>{completion_time}</div></div>
        <div class='kpi-box {'urgent' if curr_weather else ''}'><div class='kpi-label'>Weather</div><div class='kpi-value'>{'SNOW' if curr_weather else 'CLEAR'}</div></div>
    </div>
    """, unsafe_allow_html=True)

    col_L, col_R = st.columns([0.65, 0.35])

    with col_L:
        st.markdown("<div class='sect-header'>Active Shift Tasks</div>", unsafe_allow_html=True)
        if open_t.empty: st.success("ALL TASKS COMPLETE")
        else:
            for _, r in open_t.iterrows():
                c1, c2, c3 = st.columns([0.6, 0.25, 0.15])
                p_style = "data-urgent" if r['Priority'] == "Urgent" else ""
                c1.markdown(f"<div class='data-card {p_style}'><div><strong>[{r['Zone']}]</strong> {html.escape(r['Task_Detail'])} <em>({r['Est_Mins']}m)</em><br><span class='assign-text'>OWNER: {r['Assigned_To']}</span></div></div>", unsafe_allow_html=True)
                
                assign_opts = ["Unassigned"] + live_active_staff
                if pd.notna(r['Assigned_To']) and r['Assigned_To'] not in assign_opts and str(r['Assigned_To']).strip() != "":
                    assign_opts.append(r['Assigned_To'])
                    
                curr_owner = r['Assigned_To'] if r['Assigned_To'] in assign_opts else "Unassigned"
                w_key = f"sel_{r['Task_ID']}" 
                safe_index = assign_opts.index(curr_owner) if curr_owner in assign_opts else 0
                
                c2.selectbox("Assign", assign_opts, index=safe_index, key=w_key, label_visibility="collapsed", on_change=handle_assign_callback, args=(r['Task_ID'], w_key))
                c3.button("DONE", key=f"dn_{r['Task_ID']}", on_click=complete_task, args=(r['Task_ID'], active_op))

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("<div class='sect-header'>Customer Orders</div>", unsafe_allow_html=True)
            open_s = s_df[s_df["Status"] == "Open"] if not s_df.empty and "Status" in s_df.columns else pd.DataFrame()
            if open_s.empty: st.caption("No pending requests.")
            for _, r in open_s.iterrows():
                cx, cy = st.columns([0.75, 0.25])
                cx.markdown(f"<div class='data-card' style='border-left-color:#a855f7; padding:8px;'><div><strong>📍 Location {r['Location']}</strong><br>{html.escape(r['Item'])}<br><span style='color:#a855f7; font-size:12px;'>👤 {html.escape(r['Customer'])}</span></div></div>", unsafe_allow_html=True)
                cy.button("P/U", key=f"s_{r['Order_ID']}", on_click=complete_special_order, args=(r['Order_ID'], active_op))

        with c2:
            st.markdown("<div class='sect-header'>Expected Inbound</div>", unsafe_allow_html=True)
            open_e = e_df[e_df["Status"] == "Pending"] if not e_df.empty and "Status" in e_df.columns else pd.DataFrame()
            if open_e.empty: st.caption("No expected freight logged.")
            for _, r in open_e.iterrows():
                cx, cy = st.columns([0.75, 0.25])
                cx.markdown(f"<div class='data-card' style='border-left-color:#f59e0b; padding:8px;'><div>🚚 <strong>{html.escape(r['Vendor'])}</strong></div></div>", unsafe_allow_html=True)
                cy.button("RCV", key=f"e_{r['Exp_ID']}", on_click=complete_expected_order, args=(r['Exp_ID'], active_op))

    with col_R:
        st.markdown("<div class='sect-header'>OOS Flags (Shelf Holes)</div>", unsafe_allow_html=True)
        open_o = oos_df[oos_df["Status"] == "Open"] if not oos_df.empty and "Status" in oos_df.columns else pd.DataFrame()
        if open_o.empty: st.caption("No holes reported.")
        for _, r in open_o.iterrows():
            c1, c2 = st.columns([0.8, 0.2])
            notes_html = f"<br><span style='color:#ef4444; font-size:12px;'>Notes: {html.escape(r['Notes'])}</span>" if pd.notna(r['Notes']) and str(r['Notes']).strip() else ""
            c1.markdown(f"<div class='data-card data-urgent' style='padding:8px;'><div><strong>{r['Zone']}:</strong> {r['Hole_Count']} Holes {notes_html}</div></div>", unsafe_allow_html=True)
            c2.button("CLR", key=f"o_{r['OOS_ID']}", on_click=complete_oos, args=(r['OOS_ID'], active_op))

        st.divider()
        if st.button("🚀 Auto-Load Daily Rhythm"):
            with get_db() as conn:
                cur = conn.cursor()
                dynamic_tgp_time = int((((curr_g_pcs + curr_f_pcs) / live_cph) / curr_staff) * 60) if (curr_g_pcs + curr_f_pcs) > 0 else 120
                directives = [
                    {"Task": "5-Minute Direction Huddle", "Priority": "Urgent", "Zone": "General", "Time": 5},
                    {"Task": "Store Walk & Documentation", "Priority": "High", "Zone": "General", "Time": 30}
                ]
                if curr_weather: directives.append({"Task": "URGENT: Snow Removal/Salt", "Priority": "Urgent", "Zone": "Outside", "Time": 20})
                if curr_day in ["Sunday", "Tuesday", "Thursday"]: directives.append({"Task": "TGP Order", "Priority": "Urgent", "Zone": "Receiving", "Time": dynamic_tgp_time})
                if curr_day == "Sunday": directives.append({"Task": "Build Displays (16hr budget)", "Priority": "High", "Zone": "General", "Time": 960})
                if curr_day == "Wednesday": directives.append({"Task": "PRIMARY AD CHANGEOVER", "Priority": "Urgent", "Zone": "General", "Time": 240})
                if curr_day == "Friday": directives.append({"Task": "Finalize Weekend Coverage", "Priority": "High", "Zone": "General", "Time": 60})

                inserted_tasks = 0
                for d in directives:
                    try:
                        cur.execute("INSERT INTO tasks VALUES (?, ?, 'Open', ?, ?, 'Unassigned', ?, ?, '', '')", 
                                     (gen_id(), d["Task"], d["Priority"], d["Zone"], d["Time"], get_utc_now()))
                        inserted_tasks += 1
                    except sqlite3.IntegrityError: pass 
                
                inserted_vendors = 0
                today_vendors = VENDOR_SCHEDULE.get(curr_day, [])
                for v in today_vendors:
                    try:
                        cur.execute("INSERT INTO expected_orders VALUES (?, ?, ?, 'Pending', 'AUTO', '', '')", 
                                     (gen_id(), v, curr_day))
                        inserted_vendors += 1
                    except sqlite3.IntegrityError: pass
                
                _internal_audit(cur, f"Auto-Load Complete: {inserted_tasks} tasks, {inserted_vendors} vendors")
            st.rerun()

    # --- LIVE MULTI-TICKER (BOTTOM) ---
    live_tk = tk_df.dropna(subset=["Message"])
    if not live_tk.empty:
        msgs = " &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; ".join(live_tk["Message"].astype(str).tolist())
        repeated_ticker = f"📢 {msgs} &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; " * 5
        st.markdown(f"<div class='ticker-wrap'><div class='ticker'>{repeated_ticker}</div></div>", unsafe_allow_html=True)

# --- DISPLAY LOGIC (TV VS MOBILE) ---
@st.fragment(run_every=3)
def auto_refresh_board():
    render_main_board()

if is_tv_mode:
    auto_refresh_board()
else:
    render_main_board()
