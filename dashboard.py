import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

# --- BOARD CONFIGURATION ---
st.set_page_config(page_title="TGP Comm Board", layout="wide", initial_sidebar_state="collapsed")

# --- ENGINE ---
def get_now(): 
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=6)

def f_time(dt): return dt.strftime("%Y-%m-%d %H:%M:%S")

# --- DATABASE ARCHITECTURE ---
DB_SCHEMA = {
    "tasks.csv": ["Task_ID", "Task_Detail", "Status", "Priority", "Zone", "Assigned_To", "Est_Mins", "Time_Submitted", "Closed_By", "Time_Closed"],
    "oos.csv": ["OOS_ID", "Zone", "Hole_Count", "Notes", "Status", "Logged_By", "Time_Logged"],
    "counts.csv": ["Grocery", "Frozen", "Staff", "Last_Update", "Weather_Alert", "Ticker_Msg"],
    "audit.csv": ["Log_ID", "Timestamp", "Event"],
    "special_orders.csv": ["Order_ID", "Customer", "Item", "Contact", "Location", "Status", "Logged_By", "Time_Logged"],
    "expected_orders.csv": ["Exp_ID", "Vendor", "Expected_Day", "Status", "Logged_By"],
    "ticker.csv": ["Msg_ID", "Message"],
    "staff.csv": ["Name", "Active"],
    "settings.csv": ["Setting_Name", "Setting_Value"]
}

# --- VENDOR DNA ---
VENDOR_SCHEDULE = {
    "Monday": ["Old Dutch", "Coke", "Pepsi", "Frito Lay (Retail)", "Frito Lay (Vending)", "Italian Bakery", "Canada Bread"],
    "Tuesday": ["TGP", "Old Dutch"],
    "Wednesday": ["Old Dutch", "Frito Lay (Retail)"],
    "Thursday": ["TGP", "Old Dutch", "Pepsi", "Frito Lay (Vending)", "Italian Bakery", "Canada Bread"],
    "Friday": ["Old Dutch", "Coke", "Frito Lay (Retail)"],
    "Saturday": [],
    "Sunday": ["TGP"]
}

# --- PREMIUM SUPERVISORS ---
PREMIUM_STAFF = ["Chris", "Ashley", "Luke", "Chandler"]

# --- ORDER LOCATIONS ---
ORDER_LOCATIONS = ["1", "2", "3", "22"]

# --- DATA INITIALIZATION & PATCHING ---
for f, cols in DB_SCHEMA.items():
    if not os.path.exists(f):
        if f == "counts.csv":
            pd.DataFrame({"Grocery": [0], "Frozen": [0], "Staff": [1], "Last_Update": [f_time(get_now())], "Weather_Alert": [False], "Ticker_Msg": [""]}).to_csv(f, index=False)
        elif f == "staff.csv":
            pd.DataFrame({"Name": ["John", "Sarah", "Mike", "Emily"], "Active": [True, True, True, True]}).to_csv(f, index=False)
        elif f == "settings.csv":
            pd.DataFrame({"Setting_Name": ["Cases_Per_Hour"], "Setting_Value": ["55"]}).to_csv(f, index=False)
        else: pd.DataFrame(columns=cols).to_csv(f, index=False)
    else:
        try:
            df = pd.read_csv(f)
            missing = [c for c in cols if c not in df.columns]
            if missing:
                for m in missing:
                    if m == "Weather_Alert": df[m] = False
                    elif m == "Est_Mins": df[m] = 15
                    elif m == "Hole_Count": df[m] = 1
                    elif m == "Active": df[m] = True
                    elif m == "Ticker_Msg": df[m] = ""
                    else: df[m] = ""
                df.to_csv(f, index=False)
        except: pass

def load_data(f): return pd.read_csv(f)
def save_data(df, f): df.to_csv(f, index=False)

# --- ACTIONS & CALLBACKS (Now mathematically bulletproof) ---
def log_audit(event):
    df = load_data("audit.csv")
    new_id = 1 if df.empty else df["Log_ID"].max() + 1
    new_row = pd.DataFrame([{"Log_ID": new_id, "Timestamp": get_now().strftime("%H:%M:%S"), "Event": event}])
    save_data(pd.concat([df, new_row], ignore_index=True).tail(50), "audit.csv")

def assign_task(task_id, staff_name):
    df = load_data("tasks.csv")
    df.loc[pd.to_numeric(df["Task_ID"], errors='coerce') == float(task_id), "Assigned_To"] = staff_name
    save_data(df, "tasks.csv")
    log_audit(f"Task #{task_id} assigned to {staff_name}")

def complete_task(task_id, premium_user, assigned_user):
    df = load_data("tasks.csv")
    df.loc[pd.to_numeric(df["Task_ID"], errors='coerce') == float(task_id), "Status"] = "Closed"
    df["Closed_By"] = df["Closed_By"].astype(object)
    df["Time_Closed"] = df["Time_Closed"].astype(object)
    df.loc[pd.to_numeric(df["Task_ID"], errors='coerce') == float(task_id), ["Closed_By", "Time_Closed"]] = [premium_user, f_time(get_now())]
    save_data(df, "tasks.csv")
    worker = assigned_user if assigned_user and assigned_user != "Unassigned" else "the Team"
    log_audit(f"Task completed by {worker} (Verified by {premium_user})")

def execute_action(file, id_col, item_id, user=None):
    df = load_data(file)
    df.loc[pd.to_numeric(df[id_col], errors='coerce') == float(item_id), "Status"] = "Closed"
    if "Closed_By" in df.columns:
        df["Closed_By"] = df["Closed_By"].astype(object)
        df["Time_Closed"] = df["Time_Closed"].astype(object)
        df.loc[pd.to_numeric(df[id_col], errors='coerce') == float(item_id), ["Closed_By", "Time_Closed"]] = [user, f_time(get_now())]
    save_data(df, file)
    log_audit(f"Cleared {id_col.replace('_ID','')} #{item_id}")

def delete_ticker(msg_id):
    df = load_data("ticker.csv")
    df = df[pd.to_numeric(df["Msg_ID"], errors='coerce') != float(msg_id)]
    save_data(df, "ticker.csv")
    log_audit("Broadcast message cleared")

def safe_int(df, col, default=0):
    if df.empty or col not in df.columns: return default
    try: return int(float(df[col].iloc[0]))
    except: return default

# --- UI STYLING & RESPONSIVE DESIGN ---
st.markdown(f"""
<style>
/* HIDE FOOTER AND DEFAULT MENU GLOBALLY */
footer {{ visibility: hidden; }}
#MainMenu {{ visibility: hidden; }}

/* GLOBAL APP BACKGROUND */
.stApp {{ background-color: #0b0f14; color: #d1d5db; }}

/* --- TV LAYOUT (DEFAULT - LARGE SCREENS) --- */
/* Hides the top bar entirely and adjusts padding to fit TV */
header {{ visibility: hidden; }}
.block-container {{ padding-top: 1rem; padding-bottom: 5rem; padding-left: 2rem; padding-right: 2rem; max-width: 100%; }}

/* --- MOBILE LAYOUT (SMALL SCREENS) --- */
/* Brings back the hamburger menu for phones and tablets */
@media screen and (max-width: 1024px) {{
    header {{ visibility: visible; background-color: #0b0f14; }}
    .block-container {{ padding-top: 4rem; padding-left: 1rem; padding-right: 1rem; }}
}}

/* KPI & CARD STYLES */
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

# --- GLOBAL STATE LOADS (FOR SIDEBAR) ---
now = get_now()
day_name = now.strftime("%A")
t_df, oos_df, c_df, a_df, s_df, e_df = load_data("tasks.csv"), load_data("oos.csv"), load_data("counts.csv"), load_data("audit.csv"), load_data("special_orders.csv"), load_data("expected_orders.csv")
tk_df, staff_df, set_df = load_data("ticker.csv"), load_data("staff.csv"), load_data("settings.csv")

aisles = ["Aisle 1", "Aisle 2", "Aisle 3", "Aisle 4", "Aisle 5", "Aisle 6", "Aisle 7", "Aisle 8", "Receiving", "Freezer", "Bakery", "Outside"]
weather_active = c_df["Weather_Alert"].iloc[0]

master_staff = staff_df["Name"].tolist()
active_staff = staff_df[staff_df["Active"] == True]["Name"].tolist()

cases_per_hour = 55.0
if not set_df[set_df["Setting_Name"] == "Cases_Per_Hour"].empty:
    try: cases_per_hour = float(set_df[set_df["Setting_Name"] == "Cases_Per_Hour"]["Setting_Value"].iloc[0])
    except: pass

# --- SIDEBAR: OPERATIONAL CONTROLS ---
with st.sidebar:
    st.markdown("### 🔧 COMM CENTER")
    active_op = st.selectbox("Premium Operator:", PREMIUM_STAFF)
    
    if st.button("🔄 Sync Board"): st.rerun()

    with st.expander("👥 Shift Roster Settings (Floor Staff)"):
        st.caption("Select floor staff working today. These names appear in the task Assign dropdowns.")
        selected_active = st.multiselect("Active Today:", master_staff, default=active_staff)
        if selected_active != active_staff:
            staff_df["Active"] = staff_df["Name"].isin(selected_active)
            save_data(staff_df, "staff.csv")
            st.rerun()
            
        st.divider()
        new_staff = st.text_input("Add New Floor Staff")
        if st.button("Add to Roster") and new_staff and new_staff not in master_staff:
            save_data(pd.concat([staff_df, pd.DataFrame([{"Name": new_staff, "Active": True}])]), "staff.csv")
            st.rerun()

    st.divider()
    st.markdown("**1. Broadcast Manager**")
    with st.form("ticker_add", clear_on_submit=True):
        new_msg = st.text_input("Add Ticker Message")
        if st.form_submit_button("Broadcast") and new_msg:
            new_id = 1 if tk_df.empty else tk_df["Msg_ID"].max() + 1
            save_data(pd.concat([tk_df, pd.DataFrame([{"Msg_ID": new_id, "Message": new_msg}])]), "ticker.csv")
            log_audit(f"Broadcast added: {new_msg}"); st.rerun()
            
    if not tk_df.empty:
        for _, r in tk_df.iterrows():
            c1, c2 = st.columns([0.85, 0.15])
            c1.caption(f"📢 {r['Message']}")
            # Use on_click callback instead of 'if button:'
            c2.button("X", key=f"tk_{r['Msg_ID']}", on_click=delete_ticker, args=(r['Msg_ID'],))

    st.divider()
    st.markdown("**2. Deploy Task**")
    with st.form("custom_task_form", clear_on_submit=True):
        t_desc = st.text_input("Task Description")
        t_zone = st.selectbox("Zone", aisles)
        t_pri = st.selectbox("Priority", ["Routine", "High", "Urgent"])
        t_est = st.number_input("Est. Time (Mins)", min_value=1, value=15, step=5)
        if st.form_submit_button("Deploy Task") and t_desc:
            new_id = 1 if t_df.empty else t_df["Task_ID"].max() + 1
            new_row = {"Task_ID": new_id, "Task_Detail": t_desc.capitalize(), "Status": "Open", "Priority": t_pri, "Zone": t_zone, "Assigned_To": "Unassigned", "Est_Mins": t_est, "Time_Submitted": f_time(now)}
            save_data(pd.concat([t_df, pd.DataFrame([new_row])]), "tasks.csv")
            log_audit(f"Task Deployed: {t_desc}"); st.rerun()

    st.divider()
    st.markdown("**3. Load / Labor Engine**")
    with st.form("load_form"):
        g_pcs = st.number_input("Grocery Pcs", value=safe_int(c_df, 'Grocery'))
        f_pcs = st.number_input("Frozen Pcs", value=safe_int(c_df, 'Frozen'))
        staff_count = st.number_input("Active Staff", min_value=1, value=safe_int(c_df, 'Staff', 1))
        if st.form_submit_button("Calculate Labor"):
            c_df.loc[0, ['Grocery', 'Frozen', 'Staff', 'Last_Update']] = [g_pcs, f_pcs, staff_count, f_time(now)]
            save_data(c_df, "counts.csv"); st.rerun()

    st.divider()
    st.markdown("**4. Log OOS Holes**")
    with st.form("oos_form", clear_on_submit=True):
        oos_zone = st.selectbox("Aisle", aisles[:8] + ["Freezer", "Bakery"])
        oos_count = st.number_input("Number of Holes", min_value=1, value=1)
        oos_notes = st.text_input("Notes (e.g., Deletes, Missing DSD)")
        if st.form_submit_button("Log Holes"):
            new_id = 1 if oos_df.empty else oos_df["OOS_ID"].max() + 1
            save_data(pd.concat([oos_df, pd.DataFrame([{"OOS_ID": new_id, "Zone": oos_zone, "Hole_Count": oos_count, "Notes": oos_notes, "Status": "Open", "Logged_By": active_op, "Time_Logged": f_time(now)}])]), "oos.csv")
            log_audit(f"Logged {oos_count} holes in {oos_zone}"); st.rerun()

    st.divider()
    st.markdown("**5. Log Customer Order**")
    with st.form("cust_order_form", clear_on_submit=True):
        c_loc = st.selectbox("Location Ordered Under", ORDER_LOCATIONS)
        c_item = st.text_input("Item")
        c_name = st.text_input("Customer Name")
        if st.form_submit_button("Log Order") and c_item and c_name:
            new_id = 1 if s_df.empty else s_df["Order_ID"].max() + 1
            save_data(pd.concat([s_df, pd.DataFrame([{"Order_ID": new_id, "Customer": c_name, "Item": c_item, "Location": c_loc, "Contact": "", "Status": "Open", "Logged_By": active_op, "Time_Logged": f_time(now)}])]), "special_orders.csv")
            log_audit(f"Customer Order Logged for Location {c_loc}"); st.rerun()

    st.divider()
    st.markdown("**6. Add Extra Inbound Vendor**")
    with st.form("exp_order_form", clear_on_submit=True):
        e_ven = st.text_input("Vendor (e.g. Direct Plus, Saputo)")
        if st.form_submit_button("Log Extra Inbound") and e_ven:
            new_id = 1 if e_df.empty else e_df["Exp_ID"].max() + 1
            save_data(pd.concat([e_df, pd.DataFrame([{"Exp_ID": new_id, "Vendor": e_ven, "Expected_Day": day_name, "Status": "Pending", "Logged_By": active_op}])]), "expected_orders.csv")
            log_audit(f"Extra Inbound Logged: {e_ven}"); st.rerun()

    st.divider()
    if st.button("🌦️ Toggle Weather Alert"):
        c_df.at[0, "Weather_Alert"] = not weather_active
        save_data(c_df, "counts.csv"); st.rerun()

    # --- ADMIN CONSOLE ---
    st.divider()
    with st.expander("🛡️ Admin Console"):
        pin = st.text_input("Admin PIN", type="password")
        if pin == "1234":
            st.success("Admin Unlocked")
            
            st.markdown("**Labor Metrics**")
            with st.form("metric_form"):
                new_cph = st.number_input("Target Cases Per Hour", value=cases_per_hour)
                if st.form_submit_button("Update Metric"):
                    set_df["Setting_Value"] = set_df["Setting_Value"].astype(object)
                    set_df.loc[set_df["Setting_Name"] == "Cases_Per_Hour", "Setting_Value"] = str(new_cph)
                    save_data(set_df, "settings.csv"); st.rerun()
            
            st.markdown("**Roster Management**")
            with st.form("del_staff_form"):
                del_staff = st.selectbox("Permanently Delete Floor Staff", master_staff)
                if st.form_submit_button("Delete"):
                    save_data(staff_df[staff_df["Name"] != del_staff], "staff.csv"); st.rerun()

            st.markdown("**Database Reset**")
            if st.button("🌙 END OF DAY RESET", type="primary"):
                save_data(t_df[t_df["Status"] == "Open"], "tasks.csv")
                save_data(oos_df.iloc[0:0], "oos.csv")
                save_data(s_df[s_df["Status"] == "Open"], "special_orders.csv")
                save_data(e_df[e_df["Status"] == "Pending"], "expected_orders.csv")
                save_data(tk_df.iloc[0:0], "ticker.csv")
                log_audit("EOD RESET COMPLETED by Admin")
                st.rerun()
        elif pin:
            st.error("Invalid PIN")


# --- MAIN DASHBOARD (TV UI WITH AUTO-REFRESH) ---
@st.fragment(run_every=10)
def live_tv_board():
    # Freshly load data to capture background changes from mobile users
    curr_now = get_now()
    curr_day = curr_now.strftime("%A")
    f_t_df = load_data("tasks.csv")
    f_oos_df = load_data("oos.csv")
    f_c_df = load_data("counts.csv")
    f_s_df = load_data("special_orders.csv")
    f_e_df = load_data("expected_orders.csv")
    f_tk_df = load_data("ticker.csv")
    f_set_df = load_data("settings.csv")
    f_staff_df = load_data("staff.csv")

    f_weather_active = f_c_df["Weather_Alert"].iloc[0]
    f_active_staff = f_staff_df[f_staff_df["Active"] == True]["Name"].tolist()
    
    f_cases_per_hour = 55.0
    if not f_set_df[f_set_df["Setting_Name"] == "Cases_Per_Hour"].empty:
        try: f_cases_per_hour = float(f_set_df[f_set_df["Setting_Name"] == "Cases_Per_Hour"]["Setting_Value"].iloc[0])
        except: pass

    st.markdown(f"<div class='header-bar'><div class='header-title'>TGP CENTRE STORE // {curr_day}</div><div style='color:#8b949e;'>{curr_now.strftime('%H:%M')}</div></div>", unsafe_allow_html=True)

    # TRUE KPI / Time to Complete Math
    g_pcs, f_pcs, staff_count = safe_int(f_c_df, 'Grocery'), safe_int(f_c_df, 'Frozen'), safe_int(f_c_df, 'Staff', 1)
    total_pcs = g_pcs + f_pcs
    freight_hours = (total_pcs / f_cases_per_hour) if f_cases_per_hour > 0 else 0

    # Factor in Task Minutes
    open_t = f_t_df[f_t_df["Status"] == "Open"].copy()
    task_mins = pd.to_numeric(open_t["Est_Mins"], errors='coerce').fillna(15).sum() if not open_t.empty else 0
    task_hours = task_mins / 60.0

    total_hours_needed = (freight_hours + task_hours) / staff_count if staff_count > 0 else 0
    completion_time = (curr_now + timedelta(hours=total_hours_needed)).strftime('%H:%M') if (total_pcs > 0 or task_mins > 0) else "N/A"

    st.markdown(f"""
    <div class='kpi-container'>
        <div class='kpi-box'><div class='kpi-label'>Load Total</div><div class='kpi-value'>{total_pcs} Pcs</div></div>
        <div class='kpi-box'><div class='kpi-label'>Active Staff</div><div class='kpi-value'>{staff_count}</div></div>
        <div class='kpi-box'><div class='kpi-label'>Task Workload</div><div class='kpi-value'>{int(task_mins)} Mins</div></div>
        <div class='kpi-box {'urgent' if total_hours_needed > 7.5 else ''}'><div class='kpi-label'>Time to Complete</div><div class='kpi-value'>{round(total_hours_needed,1)} Hrs</div></div>
        <div class='kpi-box'><div class='kpi-label'>True ETA</div><div class='kpi-value' style='color:#00e676;'>{completion_time}</div></div>
        <div class='kpi-box {'urgent' if f_weather_active else ''}'><div class='kpi-label'>Weather</div><div class='kpi-value'>{'SNOW' if f_weather_active else 'CLEAR'}</div></div>
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
                c1.markdown(f"<div class='data-card {p_style}'><div><strong>[{r['Zone']}]</strong> {r['Task_Detail']} <em>({r['Est_Mins']}m)</em><br><span class='assign-text'>OWNER: {r['Assigned_To']}</span></div></div>", unsafe_allow_html=True)
                
                new_owner = c2.selectbox("Assign", ["Assign..."] + f_active_staff, key=f"sel_{r['Task_ID']}", label_visibility="collapsed")
                if new_owner != "Assign...":
                    assign_task(r['Task_ID'], new_owner); st.rerun()
                
                # Use on_click callback
                c3.button("DONE", key=f"dn_{r['Task_ID']}", on_click=complete_task, args=(r['Task_ID'], active_op, r['Assigned_To']))

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("<div class='sect-header'>Customer Orders</div>", unsafe_allow_html=True)
            open_s = f_s_df[f_s_df["Status"] == "Open"]
            if open_s.empty: st.caption("No pending requests.")
            for _, r in open_s.iterrows():
                cx, cy = st.columns([0.75, 0.25])
                cx.markdown(f"<div class='data-card' style='border-left-color:#a855f7; padding:8px;'><div><strong>📍 Location {r['Location']}</strong><br>{r['Item']}<br><span style='color:#a855f7; font-size:12px;'>👤 {r['Customer']}</span></div></div>", unsafe_allow_html=True)
                # Use on_click callback
                cy.button("P/U", key=f"s_{r['Order_ID']}", on_click=execute_action, args=("special_orders.csv", "Order_ID", r['Order_ID'], active_op))

        with c2:
            st.markdown("<div class='sect-header'>Expected Inbound</div>", unsafe_allow_html=True)
            open_e = f_e_df[f_e_df["Status"] == "Pending"]
            if open_e.empty: st.caption("No expected freight logged.")
            for _, r in open_e.iterrows():
                cx, cy = st.columns([0.75, 0.25])
                cx.markdown(f"<div class='data-card' style='border-left-color:#f59e0b; padding:8px;'><div>🚚 <strong>{r['Vendor']}</strong></div></div>", unsafe_allow_html=True)
                # Use on_click callback
                cy.button("RCV", key=f"e_{r['Exp_ID']}", on_click=execute_action, args=("expected_orders.csv", "Exp_ID", r['Exp_ID'], active_op))

    with col_R:
        st.markdown("<div class='sect-header'>OOS Flags (Shelf Holes)</div>", unsafe_allow_html=True)
        open_o = f_oos_df[f_oos_df["Status"] == "Open"]
        if open_o.empty: st.caption("No holes reported.")
        for _, r in open_o.iterrows():
            c1, c2 = st.columns([0.8, 0.2])
            notes_html = f"<br><span style='color:#ef4444; font-size:12px;'>Notes: {r['Notes']}</span>" if r['Notes'] else ""
            c1.markdown(f"<div class='data-card data-urgent' style='padding:8px;'><div><strong>{r['Zone']}:</strong> {r['Hole_Count']} Holes {notes_html}</div></div>", unsafe_allow_html=True)
            # Use on_click callback
            c2.button("CLR", key=f"o_{r['OOS_ID']}", on_click=execute_action, args=("oos.csv", "OOS_ID", r['OOS_ID'], active_op))

        st.divider()
        if st.button("🚀 Auto-Load Daily Rhythm"):
            directives = [
                {"Task": "5-Minute Direction Huddle", "Priority": "Urgent", "Zone": "General", "Time": 5},
                {"Task": "Store Walk & Documentation", "Priority": "High", "Zone": "General", "Time": 30}
            ]
            if f_weather_active: directives.append({"Task": "URGENT: Snow Removal/Salt", "Priority": "Urgent", "Zone": "Outside", "Time": 20})
            if curr_day in ["Sunday", "Tuesday", "Thursday"]: directives.append({"Task": "TGP DELIVERY SURGE", "Priority": "Urgent", "Zone": "Receiving", "Time": 120})
            if curr_day == "Sunday": directives.append({"Task": "Build Displays (16hr budget)", "Priority": "High", "Zone": "General", "Time": 960})
            if curr_day == "Wednesday": directives.append({"Task": "PRIMARY AD CHANGEOVER", "Priority": "Urgent", "Zone": "General", "Time": 240})
            if curr_day == "Friday": directives.append({"Task": "Finalize Weekend Coverage", "Priority": "High", "Zone": "General", "Time": 60})

            curr_t = load_data("tasks.csv")
            new_t = []
            for d in directives:
                new_id = (curr_t["Task_ID"].max() if not curr_t.empty else 0) + len(new_t) + 1
                new_t.append({"Task_ID": new_id, "Task_Detail": d["Task"], "Status": "Open", "Priority": d["Priority"], "Zone": d["Zone"], "Assigned_To": "Unassigned", "Est_Mins": d["Time"], "Time_Submitted": f_time(curr_now), "Closed_By": "", "Time_Closed": ""})
            save_data(pd.concat([curr_t, pd.DataFrame(new_t)]), "tasks.csv")
            
            today_vendors = VENDOR_SCHEDULE.get(curr_day, [])
            curr_e = load_data("expected_orders.csv")
            new_e = []
            for v in today_vendors:
                if curr_e[(curr_e["Vendor"] == v) & (curr_e["Status"] == "Pending")].empty:
                    new_id = (curr_e["Exp_ID"].max() if not curr_e.empty else 0) + len(new_e) + 1
                    new_e.append({"Exp_ID": new_id, "Vendor": v, "Expected_Day": curr_day, "Status": "Pending", "Logged_By": "AUTO"})
            if new_e: save_data(pd.concat([curr_e, pd.DataFrame(new_e)]), "expected_orders.csv")

            log_audit(f"Loaded {curr_day} Rhythm & Vendors")
            st.rerun()

    # --- LIVE MULTI-TICKER (BOTTOM) ---
    if not f_tk_df.empty:
        msgs = " &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; ".join(f_tk_df["Message"].tolist())
        repeated_ticker = f"📢 {msgs} &nbsp;&nbsp;&nbsp;&nbsp; 🛑 &nbsp;&nbsp;&nbsp;&nbsp; " * 5
        st.markdown(f"<div class='ticker-wrap'><div class='ticker'>{repeated_ticker}</div></div>", unsafe_allow_html=True)

# Call the function to render the UI
live_tv_board()
