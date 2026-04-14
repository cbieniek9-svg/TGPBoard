import streamlit as st
import pandas as pd
import uuid
import hashlib
import html
from datetime import datetime, timedelta, timezone
from sqlalchemy import text

# -------------------------
# TIMEZONE & HELPERS
# -------------------------
try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Edmonton")
except ImportError:
    import pytz
    LOCAL_TZ = pytz.timezone("America/Edmonton")

def now_local():
    return datetime.now(LOCAL_TZ)

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def gen_id():
    return uuid.uuid4().hex

def hash_pin(pin):
    return hashlib.sha256(str(pin).encode()).hexdigest()

# -------------------------
# CONFIG & CSS (V1 DENSE TV LAYOUT)
# -------------------------
st.set_page_config(page_title="TGP Comm Board", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
footer { visibility: hidden; }
#MainMenu { visibility: hidden; }
.stApp { background-color: #0b0f14; color: #d1d5db; overflow-x: hidden; }

/* HEADER TOGGLE FIX */
@media screen and (min-width: 1025px) {
    header[data-testid="stHeader"] { visibility: hidden; }
    .block-container { padding-top: 0.5rem; padding-bottom: 3rem; padding-left: 1rem; padding-right: 1rem; max-width: 98vw !important; margin: auto; }
}

@media screen and (max-width: 1024px) {
    header[data-testid="stHeader"] { visibility: visible !important; background-color: #0b0f14; }
    .block-container { padding-top: 3rem; padding-bottom: 5rem; padding-left: 0.5rem; padding-right: 0.5rem; max-width: 100vw !important; }
}

div[data-testid="column"] { min-width: 0 !important; }
div[data-testid="stVerticalBlock"] { gap: 0.3rem !important; }

.header-bar { display: flex; align-items: center; justify-content: flex-end; border-bottom: 2px solid #38bdf8; margin-bottom: 10px; padding-bottom: 10px; padding-top: 10px; position: relative; }
.header-title { font-size: 36px; font-weight: 900; color: #f9fafb; position: absolute; left: 50%; transform: translateX(-50%); text-transform: uppercase; letter-spacing: 2px; margin: 0; white-space: nowrap; }
.header-time { color: #8b949e; font-size: 24px; font-weight: bold; margin: 0; z-index: 1; }

.kpi-container { display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: 5px; margin-bottom: 10px; width: 100%; }
.kpi-box { background: #161b22; border-top: 3px solid #38bdf8; padding: 5px; border-radius: 4px; text-align:center; overflow: hidden; }
.kpi-box.urgent { border-top-color: #ef4444; }
.kpi-label { font-size: 10px; font-weight: 700; color: #8b949e; text-transform: uppercase; margin-bottom: 2px; white-space: nowrap; text-overflow: ellipsis; overflow: hidden; }
.kpi-value { font-size: 15px; font-weight: 800; color: #f9fafb; white-space: nowrap; }

.data-card { background: #161b22; border-left: 4px solid #38bdf8; padding: 4px 8px; margin-bottom: 4px; border-radius: 3px; font-size: 13px; line-height: 1.3; overflow-wrap: break-word; }
.data-urgent { border-left-color: #ef4444; background: rgba(239, 68, 68, 0.05); }
.sect-header { font-size: 14px; font-weight: 700; color: #38bdf8; border-bottom: 1px solid #30363d; padding-bottom: 2px; margin: 5px 0 5px 0; text-transform: uppercase; white-space: nowrap; text-overflow: ellipsis; overflow: hidden; }

.ticker-wrap { width: 100%; overflow: hidden; background-color: #facc15; border-top: 2px solid #ca8a04; padding: 6px 0; position: fixed; bottom: 0; left: 0; z-index: 999; }
.ticker { display: inline-block; white-space: nowrap; padding-left: 100%; animation: ticker 30s linear infinite; color: #000; font-family: 'Arial Black', sans-serif; font-size: 16px; text-transform: uppercase; font-weight: bold; }
@keyframes ticker { 0% { transform: translate3d(0, 0, 0); } 100% { transform: translate3d(-100%, 0, 0); } }

div[data-testid="stButton"] > button { border-radius: 3px; border: 1px solid #30363d; background: #21262d; color: #c9d1d9; font-weight: 700; width: 100%; padding: 0px 5px !important; min-height: 26px !important; font-size: 12px !important; line-height: 1.2 !important; white-space: nowrap; }
div[data-testid="stButton"] > button:hover { border-color: #38bdf8; color: #38bdf8; }
</style>
""", unsafe_allow_html=True)

# -------------------------
# CONSTANTS
# -------------------------
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

query_params = st.query_params
is_tv_url_mode = str(query_params.get("tvmode", "")).lower() in ["true", "1", "yes"]

# -------------------------
# DATABASE POOL & LOAD LAYER
# -------------------------
conn = st.connection("postgresql", type="sql", url=st.secrets["DB_URL"])

@st.cache_data(ttl=2)
def load_raw():
    return {
        "tasks": conn.query("SELECT * FROM tasks WHERE Status='Open'", ttl=0),
        "oos": conn.query("SELECT * FROM oos WHERE Status='Open'", ttl=0),
        "orders": conn.query("SELECT * FROM special_orders WHERE Status='Open'", ttl=0),
        "expected": conn.query("SELECT * FROM expected_orders WHERE Status='Pending'", ttl=0),
        "counts": conn.query("SELECT * FROM counts WHERE ID=1", ttl=0),
        "staff": conn.query("SELECT * FROM staff", ttl=0),
        "settings": conn.query("SELECT * FROM settings", ttl=0),
        "ticker": conn.query("SELECT * FROM ticker", ttl=0)
    }

def clear_cache():
    load_raw.clear()

# Pull data for the sidebar controls immediately
raw_data = load_raw()
c_df = raw_data["counts"]
staff_df = raw_data["staff"]
set_df = raw_data["settings"]

g_pcs = int(c_df["grocery"].iloc[0]) if not c_df.empty else 0
f_pcs = int(c_df["frozen"].iloc[0]) if not c_df.empty else 0
staff_count = max(1, int(c_df["staff"].iloc[0])) if not c_df.empty else 1

master_staff = staff_df[staff_df["name"] != "Unassigned"]["name"].tolist() if not staff_df.empty else PREMIUM_STAFF
active_staff = staff_df[(staff_df["active"] == 1) & (staff_df["name"] != "Unassigned")]["name"].tolist() if not staff_df.empty else PREMIUM_STAFF

admin_pin_hash = ""
cases_per_hour = 55.0
if not set_df.empty:
    pin_val = set_df.loc[set_df["setting_name"] == "Admin_PIN", "setting_value"]
    if not pin_val.empty: admin_pin_hash = pin_val.iloc[0]
    
    cph_val = set_df.loc[set_df["setting_name"] == "Cases_Per_Hour", "setting_value"]
    if not cph_val.empty: cases_per_hour = float(cph_val.iloc[0])

# -------------------------
# SIDEBAR OPERATIONAL CONTROLS (V1 INPUT FORMS)
# -------------------------
with st.sidebar:
    st.markdown("### 🔧 COMM CENTER")
    
    tv_toggle = st.toggle("📺 Local TV Display Mode", key="tv_toggle")
    should_auto_refresh = is_tv_url_mode or tv_toggle
    
    active_op = st.selectbox("Premium Operator:", PREMIUM_STAFF)
    
    if not should_auto_refresh:
        if st.button("🔄 Sync Board Now"):
            clear_cache()
            st.rerun()

    with st.expander("👥 Shift Roster Settings"):
        selected_active = st.multiselect("Active Today:", master_staff, default=active_staff)
        if st.button("Update Roster"):
            with conn.session as s:
                s.execute(text("UPDATE staff SET Active = 0 WHERE Name != 'Unassigned'"))
                if selected_active:
                    s.execute(
                        text("UPDATE staff SET Active = 1 WHERE Name = ANY(:names)"), 
                        {"names": selected_active}
                    )
                s.commit()
            clear_cache()
            st.rerun()

    st.divider()

    with st.form("ticker_form", clear_on_submit=True):
        new_msg = st.text_input("Add Ticker Message")
        if st.form_submit_button("Broadcast") and new_msg.strip():
            with conn.session as s:
                s.execute(
                    text("INSERT INTO ticker VALUES (:id, :msg)"), 
                    {"id": gen_id(), "msg": html.escape(new_msg.strip())}
                )
                s.commit()
            clear_cache()
            st.rerun()

    with st.form("task_form", clear_on_submit=True):
        t_desc = st.text_input("Task Description")
        t_zone = st.selectbox("Zone", aisles)
        t_pri = st.selectbox("Priority", ["Routine", "High", "Urgent"])
        t_est = st.number_input("Est. Time (Mins)", min_value=1, value=15, step=5)
        if st.form_submit_button("Deploy Task") and t_desc.strip():
            with conn.session as s:
                s.execute(
                    text("""INSERT INTO tasks (Task_ID, Task_Detail, Status, Priority, Zone, Assigned_To, Est_Mins, Time_Submitted, Closed_By, Time_Closed) 
                            VALUES (:id, :desc, 'Open', :pri, :zone, 'Unassigned', :mins, :time, '', '') ON CONFLICT DO NOTHING"""),
                    {"id": gen_id(), "desc": html.escape(t_desc.strip().capitalize()), "pri": t_pri, "zone": t_zone, "mins": t_est, "time": now_utc_iso()}
                )
                s.commit()
            clear_cache()
            st.rerun()

    with st.form("load_form"):
        in_g = st.number_input("Grocery Pcs", min_value=0, value=g_pcs)
        in_f = st.number_input("Frozen Pcs", min_value=0, value=f_pcs)
        in_s = st.number_input("Active Staff", min_value=1, value=staff_count)
        if st.form_submit_button("Calculate Labor"):
            with conn.session as s:
                s.execute(
                    text("UPDATE counts SET Grocery=:g, Frozen=:f, Staff=:staff, Last_Update=:time WHERE ID = 1"),
                    {"g": in_g, "f": in_f, "staff": in_s, "time": now_utc_iso()}
                )
                s.commit()
            clear_cache()
            st.rerun()

    with st.form("oos_form", clear_on_submit=True):
        o_z = st.selectbox("Aisle", aisles[:8] + ["Freezer", "Bakery"])
        o_c = st.number_input("Number of Holes", min_value=1, value=1)
        o_n = st.text_input("Notes (e.g., Deletes)")
        if st.form_submit_button("Log Holes"):
            with conn.session as s:
                s.execute(
                    text("""INSERT INTO oos (OOS_ID, Zone, Hole_Count, Notes, Status, Logged_By, Time_Logged, Closed_By, Time_Closed) 
                            VALUES (:id, :zone, :count, :notes, 'Open', :user, :time, '', '')"""),
                    {"id": gen_id(), "zone": o_z, "count": o_c, "notes": html.escape(o_n.strip()), "user": active_op, "time": now_utc_iso()}
                )
                s.commit()
            clear_cache()
            st.rerun()

    with st.form("order_form", clear_on_submit=True):
        c_loc = st.selectbox("Location Ordered Under", ORDER_LOCATIONS)
        c_item = st.text_input("Item")
        c_name = st.text_input("Customer Name")
        if st.form_submit_button("Log Order") and c_item.strip() and c_name.strip():
            with conn.session as s:
                s.execute(
                    text("""INSERT INTO special_orders (Order_ID, Customer, Item, Contact, Location, Status, Logged_By, Time_Logged, Closed_By, Time_Closed) 
                            VALUES (:id, :cust, :item, '', :loc, 'Open', :user, :time, '', '')"""),
                    {"id": gen_id(), "cust": html.escape(c_name.strip()), "item": html.escape(c_item.strip()), "loc": c_loc, "user": active_op, "time": now_utc_iso()}
                )
                s.commit()
            clear_cache()
            st.rerun()

    with st.form("vendor_form", clear_on_submit=True):
        e_ven = st.text_input("Vendor (e.g. Saputo)")
        if st.form_submit_button("Log Inbound") and e_ven.strip():
            with conn.session as s:
                s.execute(
                    text("""INSERT INTO expected_orders (Exp_ID, Vendor, Expected_Day, Status, Logged_By, Closed_By, Time_Closed) 
                            VALUES (:id, :ven, :day, 'Pending', :user, '', '') ON CONFLICT DO NOTHING"""),
                    {"id": gen_id(), "ven": html.escape(e_ven.strip()), "day": now_local().strftime("%A"), "user": active_op}
                )
                s.commit()
            clear_cache()
            st.rerun()

# -------------------------
# DATABASE WRITE ACTIONS (V2 SAFE LAYER)
# -------------------------
def assign_task(task_id, widget_key):
    staff = st.session_state[widget_key]
    with conn.session as s:
        s.execute(text("UPDATE tasks SET Assigned_To=:staff WHERE Task_ID=:id"), {"staff": staff, "id": str(task_id)})
        s.commit()
    clear_cache()

def complete_task(task_id, user):
    with conn.session as s:
        s.execute(
            text("UPDATE tasks SET Status='Closed', Closed_By=:user, Time_Closed=:time WHERE Task_ID=:id"),
            {"user": user, "time": now_utc_iso(), "id": str(task_id)}
        )
        s.commit()
    clear_cache()

def complete_oos(oos_id, user):
    with conn.session as s:
        s.execute(
            text("UPDATE oos SET Status='Closed', Closed_By=:user, Time_Closed=:time WHERE OOS_ID=:id"),
            {"user": user, "time": now_utc_iso(), "id": str(oos_id)}
        )
        s.commit()
    clear_cache()

def complete_special_order(order_id, user):
    with conn.session as s:
        s.execute(
            text("UPDATE special_orders SET Status='Closed', Closed_By=:user, Time_Closed=:time WHERE Order_ID=:id"),
            {"user": user, "time": now_utc_iso(), "id": str(order_id)}
        )
        s.commit()
    clear_cache()

def complete_expected_order(exp_id, user):
    with conn.session as s:
        s.execute(
            text("UPDATE expected_orders SET Status='Closed', Closed_By=:user, Time_Closed=:time WHERE Exp_ID=:id"),
            {"user": user, "time": now_utc_iso(), "id": str(exp_id)}
        )
        s.commit()
    clear_cache()

def load_daily_rhythm(grocery_pcs, frozen_pcs, staff_num, cph):
    with conn.session as s:
        hrs_math = (((grocery_pcs + frozen_pcs) / cph) / staff_num) * 60 if (grocery_pcs + frozen_pcs) > 0 else 120
        curr_day = now_local().strftime('%A')
        
        ds = [{"Task": "Direction Huddle", "Priority": "Urgent", "Zone": "General", "Time": 5}, 
              {"Task": "Store Walk", "Priority": "High", "Zone": "General", "Time": 30}]
        
        if curr_day in ["Sunday", "Tuesday", "Thursday"]: ds.append({"Task": "TGP Order", "Priority": "Urgent", "Zone": "Receiving", "Time": int(hrs_math)})
        if curr_day == "Sunday": ds.append({"Task": "Build Displays (16hr budget)", "Priority": "High", "Zone": "General", "Time": 960})
        if curr_day == "Wednesday": ds.append({"Task": "PRIMARY AD CHANGEOVER", "Priority": "Urgent", "Zone": "General", "Time": 240})
        if curr_day == "Friday": ds.append({"Task": "Finalize Weekend Coverage", "Priority": "High", "Zone": "General", "Time": 60})
        
        for d in ds:
            s.execute(text("""INSERT INTO tasks (Task_ID, Task_Detail, Status, Priority, Zone, Assigned_To, Est_Mins, Time_Submitted, Closed_By, Time_Closed) 
                              VALUES (:id, :desc, 'Open', :pri, :zone, 'Unassigned', :mins, :time, '', '') ON CONFLICT DO NOTHING"""),
                      {"id": gen_id(), "desc": d["Task"], "pri": d["Priority"], "zone": d["Zone"], "mins": d["Time"], "time": now_utc_iso()})
        
        for v in VENDOR_SCHEDULE.get(curr_day, []):
            s.execute(text("""INSERT INTO expected_orders (Exp_ID, Vendor, Expected_Day, Status, Logged_By, Closed_By, Time_Closed) 
                              VALUES (:id, :ven, :day, 'Pending', 'AUTO', '', '') ON CONFLICT DO NOTHING"""),
                      {"id": gen_id(), "ven": v, "day": curr_day})
        s.commit()
    clear_cache()

# -------------------------
# RENDER LAYER (V1 TV HTML LAYOUT)
# -------------------------
def render_main_board(data_snapshot, user, is_tv):
    t_df = data_snapshot["tasks"]
    oos_df = data_snapshot["oos"]
    s_df = data_snapshot["orders"]
    e_df = data_snapshot["expected"]
    tk_df = data_snapshot["ticker"]
    
    curr_now = now_local()
    
    st.markdown(f"""
    <div class='header-bar'>
        <div class='header-title'>TGP CENTRE STORE // {curr_now.strftime('%A')}</div>
        <div class='header-time'>{curr_now.strftime('%b %d, %Y')} &nbsp;|&nbsp; {curr_now.strftime('%I:%M %p')}</div>
    </div>
    """, unsafe_allow_html=True)

    g, f = g_pcs, f_pcs
    total_pcs = g + f
    f_hrs = (total_pcs / cases_per_hour)
    t_mins = pd.to_numeric(t_df["est_mins"], errors='coerce').fillna(15).sum()
    total_hrs = (f_hrs + (t_mins / 60.0)) / staff_count
    eta = (curr_now + timedelta(hours=total_hrs)).strftime('%I:%M %p') if (total_pcs > 0 or t_mins > 0) else "N/A"

    st.markdown(f"""
    <div class='kpi-container'>
        <div class='kpi-box'><div class='kpi-label'>Grocery</div><div class='kpi-value'>{g} Pcs</div></div>
        <div class='kpi-box'><div class='kpi-label'>Frozen</div><div class='kpi-value'>{f} Pcs</div></div>
        <div class='kpi-box'><div class='kpi-label'>Staff</div><div class='kpi-value'>{staff_count}</div></div>
        <div class='kpi-box'><div class='kpi-label'>Tasks</div><div class='kpi-value'>{int(t_mins)}m</div></div>
        <div class='kpi-box {'urgent' if total_hrs > 7.5 else ''}'><div class='kpi-label'>Needed</div><div class='kpi-value'>{round(total_hrs,1)}h</div></div>
        <div class='kpi-box'><div class='kpi-label'>True ETA</div><div class='kpi-value' style='color:#00e676;'>{eta}</div></div>
    </div>
    """, unsafe_allow_html=True)

    L, R = st.columns([0.65, 0.35], gap="small")
    
    with L:
        st.markdown("<div class='sect-header'>Tasks</div>", unsafe_allow_html=True)
        if t_df.empty: st.success("All tasks complete!")
        for _, r in t_df.iterrows():
            card_html = f"<div class='data-card {'data-urgent' if r['priority'] == 'Urgent' else ''}'><strong>[{r['zone']}]</strong> {html.escape(r['task_detail'])} ({r['est_mins']}m)<br><small>OWNER: {r['assigned_to']}</small></div>"
            
            if is_tv:
                st.markdown(card_html, unsafe_allow_html=True)
            else:
                c1, c2, c3 = st.columns([0.55, 0.30, 0.15], gap="small")
                c1.markdown(card_html, unsafe_allow_html=True)
                opts = ["Unassigned"] + active_staff
                if r['assigned_to'] not in opts: opts.append(r['assigned_to'])
                c2.selectbox("Assign", opts, index=opts.index(r['assigned_to']), key=f"sel_{r['task_id']}", label_visibility="collapsed", on_change=assign_task, args=(r['task_id'], f"sel_{r['task_id']}"))
                c3.button("DONE", key=f"dn_{r['task_id']}", on_click=complete_task, args=(r['task_id'], user))

    with R:
        st.markdown("<div class='sect-header'>Shelf Holes (OOS)</div>", unsafe_allow_html=True)
        if oos_df.empty: st.caption("No holes reported.")
        for _, r in oos_df.iterrows():
            oos_html = f"<div class='data-card data-urgent'><strong>{r['zone']}</strong>: {r['hole_count']} Holes<br><small>{html.escape(r.get('notes', ''))}</small></div>"
            if is_tv:
                st.markdown(oos_html, unsafe_allow_html=True)
            else:
                c1, c2 = st.columns([0.75, 0.25], gap="small")
                c1.markdown(oos_html, unsafe_allow_html=True)
                c2.button("CLR", key=f"o_{r['oos_id']}", on_click=complete_oos, args=(r['oos_id'], user))

        st.markdown("<div class='sect-header'>Orders & Freight</div>", unsafe_allow_html=True)
        c_ord, c_exp = st.columns(2, gap="small")
        with c_ord:
            if s_df.empty: st.caption("No pending requests.")
            for _, r in s_df.iterrows():
                ord_html = f"<div class='data-card' style='border-left-color:#a855f7;'><strong>Loc {r['location']}</strong>: {html.escape(r['item'])}<br><small>{html.escape(r['customer'])}</small></div>"
                c_ord.markdown(ord_html, unsafe_allow_html=True)
                if not is_tv: c_ord.button("PU", key=f"s_{r['order_id']}", on_click=complete_special_order, args=(r['order_id'], user))
                    
        with c_exp:
            if e_df.empty: st.caption("No expected freight.")
            for _, r in e_df.iterrows():
                exp_html = f"<div class='data-card' style='border-left-color:#f59e0b;'>🚚 <strong>{html.escape(r['vendor'])}</strong></div>"
                c_exp.markdown(exp_html, unsafe_allow_html=True)
                if not is_tv: c_exp.button("RCV", key=f"e_{r['exp_id']}", on_click=complete_expected_order, args=(r['exp_id'], user))

        st.divider()
        if not is_tv:
            if st.button("🚀 Load Daily Rhythm"):
                load_daily_rhythm(g, f, staff_count, cases_per_hour)
                st.rerun()

    if not tk_df.empty:
        live_tk = tk_df.dropna(subset=["message"])
        if not live_tk.empty:
            m_str = " &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; ".join(live_tk["message"].tolist())
            st.markdown(f"<div class='ticker-wrap'><div class='ticker'>📢 {m_str} &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; </div></div>", unsafe_allow_html=True)

# -------------------------
# ENTRYPOINT & FRAGMENT LOOP
# -------------------------
if should_auto_refresh:
    @st.fragment(run_every=4)
    def tv_loop():
        # Inside the fragment, pull fresh data and render
        fresh_data = load_raw()
        render_main_board(fresh_data, active_op, is_tv=True)
    tv_loop()
else:
    # Outside the fragment, interactive rendering
    fresh_data = load_raw()
    render_main_board(fresh_data, active_op, is_tv=False)
