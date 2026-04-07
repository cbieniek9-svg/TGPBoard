import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Center Store Communication Board", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM TV STYLING ---
st.markdown("""
    <style>
    .task-routine { font-size: 28px; padding: 15px; background-color: #1e1e1e; border-left: 5px solid #ff9800; border-radius: 5px; margin-bottom: 5px; color: #ffffff; }
    .task-high { font-size: 28px; padding: 15px; background-color: #1e1e1e; border-left: 5px solid #e91e63; border-radius: 5px; margin-bottom: 5px; color: #ffffff; }
    .task-urgent { font-size: 32px; font-weight: bold; padding: 15px; background-color: #3b0000; border-left: 8px solid #ff0000; border-right: 8px solid #ff0000; border-radius: 5px; margin-bottom: 5px; color: #ffffff; }
    .order-box { font-size: 28px; padding: 15px; background-color: #1e1e1e; border-left: 5px solid #9c27b0; border-radius: 5px; margin-bottom: 5px; color: #ffffff; }
    .log-box { font-size: 24px; padding: 20px; background-color: #1a237e; border-left: 5px solid #5c6bc0; border-radius: 5px; margin-bottom: 20px; color: #ffffff; }
    .big-number { font-size: 64px; font-weight: bold; text-align: center; margin-bottom: 0px; }
    .est-time { font-size: 24px; text-align: center; color: #aaaaaa; margin-top: -10px; margin-bottom: 20px; }
    .zone-header { color: #4CAF50; font-size: 32px; margin-top: 20px; margin-bottom: 10px; border-bottom: 2px solid #333; padding-bottom: 5px; }
    .ticker-wrap { width: 100%; background-color: #111111; padding: 15px 0; border-top: 4px solid #ff9800; border-bottom: 4px solid #ff9800; margin-top: 40px; margin-bottom: 40px;}
    .ticker-text { font-size: 36px; font-weight: bold; color: #ffffff; }
    .kudos-text { color: #FFD700; } 
    .alert-text { color: #FF5252; } 
    div[data-testid="stButton"] > button { height: 60px; width: 100%; font-size: 24px; }
    </style>
""", unsafe_allow_html=True)

# --- DATA MANAGEMENT ---
TASKS_FILE = "tasks.csv"
COUNTS_FILE = "counts.csv"
ORDERS_FILE = "orders.csv"
TICKER_FILE = "ticker.csv"
LOGS_FILE = "logs.csv"

def get_yeg_now():
    return datetime.now(timezone.utc) - timedelta(hours=6)

# Initialize all databases
for f, cols in {TASKS_FILE: ["Task_ID", "Task_Detail", "Status", "Priority", "Zone", "Submitted_By", "Time_Submitted", "Closed_By", "Time_Closed"], 
                ORDERS_FILE: ["Order_ID", "Order_Detail", "Status"],
                TICKER_FILE: ["Message_ID", "Message", "Type"],
                LOGS_FILE: ["Log_ID", "Shift", "Rating", "Notes", "Submitted_By", "Time_Submitted"],
                COUNTS_FILE: ["Grocery", "Frozen", "Staff"]}.items():
    if not os.path.exists(f):
        pd.DataFrame(columns=cols).to_csv(f, index=False)
        if f == COUNTS_FILE: pd.DataFrame({"Grocery": [0], "Frozen": [0], "Staff": [1]}).to_csv(f, index=False)

def load_tasks(): return pd.read_csv(TASKS_FILE)
def load_counts(): return pd.read_csv(COUNTS_FILE)
def load_orders(): return pd.read_csv(ORDERS_FILE)
def load_ticker(): return pd.read_csv(TICKER_FILE)
def load_logs(): return pd.read_csv(LOGS_FILE)

def save_tasks(df): df.to_csv(TASKS_FILE, index=False)
def save_orders(df): df.to_csv(ORDERS_FILE, index=False)
def save_ticker(df): df.to_csv(TICKER_FILE, index=False)
def save_logs(df): df.to_csv(LOGS_FILE, index=False)

def delete_task(task_id_to_close, closer_name):
    df = load_tasks()
    now_str = get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")
    df["Closed_By"] = df["Closed_By"].astype("object")
    df["Time_Closed"] = df["Time_Closed"].astype("object")
    df.loc[df["Task_ID"] == task_id_to_close, "Status"] = "Closed"
    df.loc[df["Task_ID"] == task_id_to_close, "Closed_By"] = closer_name
    df.loc[df["Task_ID"] == task_id_to_close, "Time_Closed"] = now_str
    save_tasks(df)

def delete_order(order_id_to_close):
    df = load_orders()
    df.loc[df["Order_ID"] == order_id_to_close, "Status"] = "Closed"
    save_orders(df)

# --- SIDEBAR ---
tasks = load_tasks()
counts = load_counts()
orders = load_orders()
ticker_msgs = load_ticker()
logs = load_logs()
zones_list = ["General", "Aisle 1", "Aisle 2", "Aisle 3", "Aisle 4", "Aisle 5", "Aisle 6", "Aisle 7", "Aisle 8", "Receiving", "Freezer", "Click and Collect", "Bakery", "Outside"]

with st.sidebar:
    st.header("📱 Control Panel")
    active_user = st.selectbox("👤 User:", ["Chris", "Ashley", "Luke", "Chandler"])
    
    st.divider()
    admin_pin = st.text_input("Admin PIN:", type="password")
    is_admin = (admin_pin == "0000") 
    
    if is_admin:
        st.warning("ADMIN MODE ACTIVE")
        st.subheader("📊 Data Export")
        st.download_button("⬇️ Export Tasks (CSV)", data=tasks.to_csv(index=False).encode('utf-8'), file_name=f"Tasks_{get_yeg_now().strftime('%Y%m%d')}.csv", mime='text/csv')
        st.download_button("⬇️ Export Shift Logs (CSV)", data=logs.to_csv(index=False).encode('utf-8'), file_name=f"Logs_{get_yeg_now().strftime('%Y%m%d')}.csv", mime='text/csv')
        
        st.divider()
        if st.button("🚨 CLEAR TICKER"): pd.DataFrame(columns=ticker_msgs.columns).to_csv(TICKER_FILE, index=False); st.rerun()
        if st.button("🚨 RESET ENTIRE BOARD"):
            pd.DataFrame(columns=tasks.columns).to_csv(TASKS_FILE, index=False)
            pd.DataFrame(columns=orders.columns).to_csv(ORDERS_FILE, index=False)
            pd.DataFrame(columns=ticker_msgs.columns).to_csv(TICKER_FILE, index=False)
            pd.DataFrame(columns=logs.columns).to_csv(LOGS_FILE, index=False)
            st.rerun()
            
    st.divider()

    with st.expander("📚 SOP & Training Hub"):
        st.markdown("**📦 Stocking Standards**\n1. Always face left-to-right.\n2. Minimum 2 units deep on all faces.\n3. Cardboard immediately into buggies, not on floor.")
        st.markdown("**🛑 Safety Protocols**\n1. Never leave a glass spill unattended. Call for cleanup.\n2. Use 3 points of contact on ladders.\n3. Baler doors must remain locked when not in use.")
        st.markdown("**💻 System Operations**\n1. Scan all out-of-stocks before 10 AM.\n2. Update special order status immediately upon receiving.")

    st.divider()

    if st.button("Push Daily Routine", type="primary"):
        routine_tasks = [
            {"Detail": "Morning Temp Checks", "Zone": "General", "Priority": "High"},
            {"Detail": "Face Baking Aisle", "Zone": "Aisle 6", "Priority": "Routine"},
            {"Detail": "Bale / Cardboard", "Zone": "Receiving", "Priority": "Routine"}
        ]
        now_str = get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")
        curr_id = 0 if tasks.empty else tasks["Task_ID"].max()
        new_rows = [{"Task_ID": curr_id+i+1, "Task_Detail": t["Detail"], "Status": "Open", "Priority": t["Priority"], "Zone": t["Zone"], "Submitted_By": active_user, "Time_Submitted": now_str} for i, t in enumerate(routine_tasks)]
        save_tasks(pd.concat([tasks, pd.DataFrame(new_rows)], ignore_index=True))
        st.rerun()

    with st.form("add_ticker_form", clear_on_submit=True):
        st.subheader("📣 Push Broadcast")
        msg_type = st.selectbox("Type:", ["Kudos 🌟", "Alert 📢"])
        msg_text = st.text_input("Message:")
        if st.form_submit_button("Send to Ticker") and msg_text.strip():
            new_m_id = 1 if ticker_msgs.empty else ticker_msgs["Message_ID"].max() + 1
            save_ticker(pd.concat([ticker_msgs, pd.DataFrame([{"Message_ID": new_m_id, "Message": msg_text.strip(), "Type": msg_type}])], ignore_index=True))
            st.rerun()

    with st.form("add_task", clear_on_submit=True):
        st.subheader("Add Task")
        p = st.selectbox("Priority:", ["Routine", "High", "Urgent"])
        z = st.selectbox("Zone:", zones_list)
        d = st.text_area("Detail:")
        if st.form_submit_button("Push Task") and d.strip():
            now_str = get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")
            new_id = 1 if tasks.empty else tasks["Task_ID"].max() + 1
            save_tasks(pd.concat([load_tasks(), pd.DataFrame([{"Task_ID": new_id, "Task_Detail": d, "Status": "Open", "Priority": p, "Zone": z, "Submitted_By": active_user, "Time_Submitted": now_str}])], ignore_index=True))
            st.rerun()

    with st.form("add_order_form", clear_on_submit=True):
        st.subheader("Log Special Order")
        order_desc = st.text_area("Item/Customer/Date:")
        if st.form_submit_button("Push Order") and order_desc.strip():
            new_o_id = 1 if orders.empty else orders["Order_ID"].max() + 1
            save_orders(pd.concat([orders, pd.DataFrame([{"Order_ID": new_o_id, "Order_Detail": order_desc.strip(), "Status": "Open"}])], ignore_index=True))
            st.rerun()

    with st.form("truck_info"):
        st.subheader("Truck & Staff")
        g = st.number_input("Grocery:", value=int(counts["Grocery"].iloc[0]))
        f = st.number_input("Frozen:", value=int(counts["Frozen"].iloc[0]))
        s = st.number_input("Staff:", min_value=1, value=int(counts["Staff"].iloc[0]))
        if st.form_submit_button("Update TV"):
            pd.DataFrame({"Grocery": [g], "Frozen": [f], "Staff": [s]}).to_csv(COUNTS_FILE, index=False)
            st.rerun()

    with st.form("shift_log", clear_on_submit=True):
        st.subheader("📝 Shift Log")
        shift_type = st.selectbox("Shift:", ["Morning", "Mid-Shift", "Close"])
        rating = st.select_slider("Shift Pulse:", options=["🔴 Brutal", "🟡 Grinding", "🟢 Smooth"])
        notes = st.text_area("Handoff Notes & Issues:")
        if st.form_submit_button("Submit Shift Log") and notes.strip():
            now_str = get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")
            new_l_id = 1 if logs.empty else logs["Log_ID"].max() + 1
            new_log = pd.DataFrame([{"Log_ID": new_l_id, "Shift": shift_type, "Rating": rating, "Notes": notes.strip(), "Submitted_By": active_user, "Time_Submitted": now_str}])
            save_logs(pd.concat([logs, new_log], ignore_index=True))
            st.rerun()

# --- MAIN DASHBOARD ---
def live_tv_board():
    local_tasks = load_tasks()
    local_counts = load_counts()
    local_orders = load_orders()
    local_ticker = load_ticker()
    local_logs = load_logs()
    
    now = get_yeg_now()
    
    st.markdown("<h1 style='text-align: center; color: #ffffff; margin-bottom: 0px;'>Center Store Communication Board</h1>", unsafe_allow_html=True)
    st.markdown(f"<h3 style='text-align: center; color: #ff9800; margin-top: 0px;'>{now.strftime('%A, %B %d')} | {now.strftime('%I:%M %p')}</h3>", unsafe_allow_html=True)
    st.markdown("---")

    total = local_counts['Grocery'].iloc[0] + local_counts['Frozen'].iloc[0]
    staff = local_counts['Staff'].iloc[0]
    est_hours = round(total / (staff * 60), 1) if total > 0 else 0
    
    c1, c2 = st.columns(2)
    c1.markdown(f"<div class='big-number' style='color: #4CAF50;'>Grocery: {local_counts['Grocery'].iloc[0]}</div>", unsafe_allow_html=True)
    c2.markdown(f"<div class='big-number' style='color: #2196F3;'>Frozen: {local_counts['Frozen'].iloc[0]}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='est-time'>Est. Completion: {est_hours} hrs ({staff} staff)</div>", unsafe_allow_html=True)
    st.markdown("---")
    
    open_t = local_tasks[local_tasks["Status"] == "Open"].copy()
    if open_t.empty:
        st.success("Floor is clear.")
    else:
        p_map = {"Urgent": 1, "High": 2, "Routine": 3}
        open_t["p_rank"] = open_t["Priority"].map(p_map)
        open_t = open_t.sort_values(["p_rank", "Task_ID"])
        
        display_order = ["General", "Aisle 1", "Aisle 2", "Aisle 3", "Aisle 4", "Aisle 5", "Aisle 6", "Aisle 7", "Aisle 8", "Bakery", "Freezer", "Receiving", "Click and Collect", "Outside"]
        for z in [z for z in display_order if z in open_t["Zone"].unique()]:
            st.markdown(f"<div class='zone-header'>📍 {z}</div>", unsafe_allow_html=True)
            for _, row in open_t[open_t["Zone"] == z].iterrows():
                cols = st.columns([0.9, 0.1])
                cols[0].markdown(f"<div class='task-{row['Priority'].lower()}'>{row['Task_Detail']}</div>", unsafe_allow_html=True)
                cols[1].button("❌", key=f"t_{row['Task_ID']}", on_click=delete_task, args=(row['Task_ID'], active_user))

    st.markdown("---")
    
    col_ord, col_log = st.columns(2)
    
    with col_ord:
        st.subheader("📦 Incoming Special Orders")
        open_orders = local_orders[local_orders["Status"] == "Open"]
        if open_orders.empty:
            st.info("No special orders pending.")
        else:
            for _, row in open_orders.iterrows():
                cols = st.columns([0.8, 0.2])
                cols[0].markdown(f"<div class='order-box'>{row['Order_Detail']}</div>", unsafe_allow_html=True)
                cols[1].button("❌", key=f"o_{row['Order_ID']}", on_click=delete_order, args=(row['Order_ID'],))

    with col_log:
        st.subheader("📝 Latest Shift Handoff")
        if local_logs.empty:
            st.info("No shift logs recorded yet.")
        else:
            last_log = local_logs.iloc[-1]
            st.markdown(f"""
                <div class='log-box'>
                    <strong>{last_log['Shift']} Shift</strong> | Pulse: {last_log['Rating']} | By: {last_log['Submitted_By']}<br>
                    <em>"{last_log['Notes']}"</em>
                </div>
            """, unsafe_allow_html=True)

    if not local_ticker.empty:
        ticker_string = ""
        for _, row in local_ticker.iterrows():
            if "Kudos" in row['Type']: ticker_string += f"<span class='kudos-text'>🌟 {row['Message']}</span> &nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;&nbsp; "
            else: ticker_string += f"<span class='alert-text'>📢 {row['Message']}</span> &nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;&nbsp; "
                
        st.markdown(f"<div class='ticker-wrap'><marquee class='ticker-text' scrollamount='8'>{ticker_string}</marquee></div>", unsafe_allow_html=True)

live_tv_board()
