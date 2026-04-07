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
    .big-number { font-size: 64px; font-weight: bold; text-align: center; margin-bottom: 0px; }
    .est-time { font-size: 24px; text-align: center; color: #aaaaaa; margin-top: -10px; margin-bottom: 20px; }
    .zone-header { color: #4CAF50; font-size: 32px; margin-top: 20px; margin-bottom: 10px; border-bottom: 2px solid #333; padding-bottom: 5px; }
    div[data-testid="stButton"] > button { height: 60px; width: 100%; font-size: 24px; }
    </style>
""", unsafe_allow_html=True)

# --- DATA MANAGEMENT ---
TASKS_FILE = "tasks.csv"
COUNTS_FILE = "counts.csv"
ORDERS_FILE = "orders.csv"

# Timezone helper for Edmonton (UTC-6)
def get_yeg_now():
    return datetime.now(timezone.utc) - timedelta(hours=6)

# Initialize files
for f, cols in {TASKS_FILE: ["Task_ID", "Task_Detail", "Status", "Priority", "Zone", "Submitted_By", "Time_Submitted", "Closed_By", "Time_Closed"], 
                ORDERS_FILE: ["Order_ID", "Order_Detail", "Status"],
                COUNTS_FILE: ["Grocery", "Frozen", "Staff"]}.items():
    if not os.path.exists(f):
        pd.DataFrame(columns=cols).to_csv(f, index=False)
        if f == COUNTS_FILE: pd.DataFrame({"Grocery": [0], "Frozen": [0], "Staff": [1]}).to_csv(f, index=False)

def load_tasks(): return pd.read_csv(TASKS_FILE)
def load_counts(): return pd.read_csv(COUNTS_FILE)
def load_orders(): return pd.read_csv(ORDERS_FILE)
def save_tasks(df): df.to_csv(TASKS_FILE, index=False)
def save_orders(df): df.to_csv(ORDERS_FILE, index=False)

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
zones_list = ["General", "Aisles 1-5", "Aisles 6-10", "Backroom", "Cooler/Freezer", "Front End"]

with st.sidebar:
    st.header("📱 Control Panel")
    active_user = st.selectbox("👤 User:", ["Chris", "Ashley", "Luke", "Chandler"])
    
    st.divider()
    admin_pin = st.text_input("Admin PIN:", type="password")
    is_admin = (admin_pin == "0000") 
    
    if is_admin:
        st.warning("ADMIN MODE")
        if st.button("🚨 RESET BOARD"):
            pd.DataFrame(columns=tasks.columns).to_csv(TASKS_FILE, index=False)
            pd.DataFrame(columns=orders.columns).to_csv(ORDERS_FILE, index=False)
            st.rerun()
    st.divider()

    if st.button("Push Daily Routine", type="primary"):
        routine_tasks = [
            {"Detail": "Morning Temp Checks", "Zone": "General", "Priority": "High"},
            {"Detail": "Face Baking Aisle", "Zone": "Aisles 6-10", "Priority": "Routine"},
            {"Detail": "Bale / Cardboard", "Zone": "Backroom", "Priority": "Routine"}
        ]
        now_str = get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")
        curr_id = 0 if tasks.empty else tasks["Task_ID"].max()
        new_rows = []
        for i, t in enumerate(routine_tasks):
            new_rows.append({"Task_ID": curr_id+i+1, "Task_Detail": t["Detail"], "Status": "Open", "Priority": t["Priority"], "Zone": t["Zone"], "Submitted_By": active_user, "Time_Submitted": now_str})
        save_tasks(pd.concat([tasks, pd.DataFrame(new_rows)], ignore_index=True))
        st.rerun()

    with st.form("add_task", clear_on_submit=True):
        st.subheader("Add Task")
        p = st.selectbox("Priority:", ["Routine", "High", "Urgent"])
        z = st.selectbox("Zone:", zones_list)
        d = st.text_area("Detail:")
        if st.form_submit_button("Push"):
            now_str = get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")
            new_id = 1 if tasks.empty else tasks["Task_ID"].max() + 1
            new_row = pd.DataFrame([{"Task_ID": new_id, "Task_Detail": d, "Status": "Open", "Priority": p, "Zone": z, "Submitted_By": active_user, "Time_Submitted": now_str}])
            save_tasks(pd.concat([load_tasks(), new_row], ignore_index=True))
            st.rerun()

    with st.form("truck_info"):
        st.subheader("Truck & Staff")
        g = st.number_input("Grocery:", value=int(counts["Grocery"].iloc[0]))
        f = st.number_input("Frozen:", value=int(counts["Frozen"].iloc[0]))
        s = st.number_input("Staff:", min_value=1, value=int(counts["Staff"].iloc[0]))
        if st.form_submit_button("Update TV"):
            pd.DataFrame({"Grocery": [g], "Frozen": [f], "Staff": [s]}).to_csv(COUNTS_FILE, index=False)
            st.rerun()

# --- MAIN DASHBOARD ---
@st.fragment(run_every="2s")
def live_tv_board():
    local_tasks = load_tasks()
    local_counts = load_counts()
    
    now = get_yeg_now()
    
    st.markdown(f"<h1 style='text-align: center; color: #ffffff; margin-bottom: 0px;'>Center Store Communication Board</h1>", unsafe_allow_html=True)
    st.markdown(f"<h3 style='text-align: center; color: #ff9800; margin-top:
