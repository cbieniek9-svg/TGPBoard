import streamlit as st
import pandas as pd
import os
from datetime import datetime

# --- PAGE CONFIGURATION FOR TV ---
st.set_page_config(page_title="Center Store Communication Board", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM TV STYLING ---
st.markdown("""
    <style>
    .task-routine {
        font-size: 28px; padding: 15px; background-color: #1e1e1e;
        border-left: 5px solid #ff9800; border-radius: 5px; margin-bottom: 5px; color: #ffffff;
    }
    .task-high {
        font-size: 28px; padding: 15px; background-color: #1e1e1e;
        border-left: 5px solid #e91e63; border-radius: 5px; margin-bottom: 5px; color: #ffffff;
    }
    .task-urgent {
        font-size: 32px; font-weight: bold; padding: 15px; background-color: #3b0000;
        border-left: 8px solid #ff0000; border-right: 8px solid #ff0000;
        border-radius: 5px; margin-bottom: 5px; color: #ffffff;
    }
    .order-box {
        font-size: 28px; padding: 15px; background-color: #1e1e1e;
        border-left: 5px solid #9c27b0; border-radius: 5px; margin-bottom: 5px; color: #ffffff;
    }
    .big-number {
        font-size: 64px; font-weight: bold; text-align: center; margin-bottom: 0px;
    }
    .est-time {
        font-size: 24px; text-align: center; color: #aaaaaa; margin-top: -10px; margin-bottom: 20px;
    }
    .zone-header {
        color: #4CAF50; font-size: 32px; margin-top: 20px; margin-bottom: 10px; border-bottom: 2px solid #333; padding-bottom: 5px;
    }
    div[data-testid="stButton"] > button {
        height: 60px; width: 100%; font-size: 24px;
    }
    </style>
""", unsafe_allow_html=True)

# --- DATA MANAGEMENT ---
TASKS_FILE = "tasks.csv"
COUNTS_FILE = "counts.csv"
ORDERS_FILE = "orders.csv"

# Safe DB Upgrades (Includes "Zone" and Accountability tracking)
if not os.path.exists(TASKS_FILE):
    pd.DataFrame(columns=["Task_ID", "Task_Detail", "Status", "Priority", "Zone", "Submitted_By", "Time_Submitted", "Closed_By", "Time_Closed"]).to_csv(TASKS_FILE, index=False)
else:
    temp_tasks = pd.read_csv(TASKS_FILE)
    new_cols = ["Priority", "Zone", "Submitted_By", "Time_Submitted", "Closed_By", "Time_Closed"]
    for col in new_cols:
        if col not in temp_tasks.columns:
            if col == "Priority": temp_tasks[col] = "Routine"
            elif col == "Zone": temp_tasks[col] = "General"
            else: temp_tasks[col] = ""
    temp_tasks.to_csv(TASKS_FILE, index=False)

if not os.path.exists(ORDERS_FILE):
    pd.DataFrame(columns=["Order_ID", "Order_Detail", "Status"]).to_csv(ORDERS_FILE, index=False)

if not os.path.exists(COUNTS_FILE):
    pd.DataFrame({"Grocery": [0], "Frozen": [0], "Staff": [1]}).to_csv(COUNTS_FILE, index=False)
else:
    temp_counts = pd.read_csv(COUNTS_FILE)
    if "Staff" not in temp_counts.columns:
        temp_counts["Staff"] = 1
        temp_counts.to_csv(COUNTS_FILE, index=False)

def load_tasks(): return pd.read_csv(TASKS_FILE)
def load_counts(): return pd.read_csv(COUNTS_FILE)
def load_orders(): return pd.read_csv(ORDERS_FILE)
def save_tasks(df): df.to_csv(TASKS_FILE, index=False)
def save_orders(df): df.to_csv(ORDERS_FILE, index=False)

def delete_task(task_id_to_close, closer_name):
    df = load_tasks()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # FIX: Force Pandas to treat these columns as text (objects) before inserting words
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

# --- SIDEBAR: MOBILE INPUT ---
tasks = load_tasks()
counts = load_counts()
orders = load_orders()
zones_list = ["General", "Aisles 1-5", "Aisles 6-10", "Backroom", "Cooler/Freezer", "Front End"]

with st.sidebar:
    st.header("📱 Control Panel")
    active_user = st.selectbox("👤 Who is managing the board?", ["Chris", "Ashley", "Luke", "Chandler"])
    st.divider()

    # 1. THE AUTO-LOADER
    st.subheader("🚀 Quick Actions")
    if st.button("Push Daily Routine", type="primary"):
        routine_tasks = [
            {"Detail": "Complete Morning Temp Checks", "Zone": "General", "Priority": "High"},
            {"Detail": "Face Baking Aisle", "Zone": "Aisles 6-10", "Priority": "Routine"},
            {"Detail": "Clear Cardboard from Floor", "Zone": "Backroom", "Priority": "Routine"},
            {"Detail": "Make a Bale", "Zone": "Backroom", "Priority": "Routine"},
            {"Detail": "Check Milk & Eggs Fill", "Zone": "Cooler/Freezer", "Priority": "High"}
        ]
        
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_rows = []
        current_max_id = 0 if tasks.empty else tasks["Task_ID"].max()
        
        for i, r_task in enumerate(routine_tasks):
            new_rows.append({
                "Task_ID": current_max_id + i + 1, "Task_Detail": r_task["Detail"], "Status": "Open", 
                "Priority": r_task["Priority"], "Zone": r_task["Zone"], 
                "Submitted_By": active_user, "Time_Submitted": now_str, "Closed_By": "", "Time_Closed": ""
            })
            
        tasks = pd.concat([tasks, pd.DataFrame(new_rows)], ignore_index=True)
        save_tasks(tasks)
        st.success("Routine Loaded!")
        st.rerun()

    st.divider()

    # 2. Add Task Form (Upgraded with Zones)
    with st.form("add_task_form", clear_on_submit=True):
        st.subheader("Add Custom Task")
        col1, col2 = st.columns(2)
        with col1:
            priority = st.selectbox("Priority:", ["Routine", "High", "Urgent"])
        with col2:
            zone = st.selectbox("Zone:", zones_list)
            
        task_desc = st.text_area("Task Detail:", height=100)
        submitted = st.form_submit_button("Push Task")
        
        if submitted and task_desc.strip():
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_id = 1 if tasks.empty else tasks["Task_ID"].max() + 1
            new_row = pd.DataFrame([{
                "Task_ID": new_id, "Task_Detail": task_desc.strip(), "Status": "Open", 
                "Priority": priority, "Zone": zone, "Submitted_By": active_user, "Time_Submitted": now_str, 
                "Closed_By": "", "Time_Closed": ""
            }])
            tasks = pd.concat([tasks, new_row], ignore_index=True)
            save_tasks(tasks)
            st.rerun()

    st.divider()
    
    # 3. Special Orders Form
    with st.form("add_order_form", clear_on_submit=True):
        st.subheader("Log Special Order")
        order_desc = st.text_area("Item/Customer/Date:", height=100)
        order_submitted = st.form_submit_button("Push Order")
        if order_submitted and order_desc.strip():
            new_o_id = 1 if orders.empty else orders["Order_ID"].max() + 1
            new_o_row = pd.DataFrame([{"Order_ID": new_o_id, "Order_Detail": order_desc.strip(), "Status": "Open"}])
            orders = pd.concat([orders, new_o_row], ignore_index=True)
            save_orders(orders)
            st.rerun()

    st.divider()

    # 4. Piece Counts Form
    with st.form("piece_counts_form"):
        st.subheader("Update Truck & Staff")
        g_count = st.number_input("Grocery Pieces:", min_value=0, value=int(counts["Grocery"].iloc[0]))
        f_count = st.number_input("Frozen Pieces:", min_value=0, value=int(counts["Frozen"].iloc[0]))
        staff_count = st.number_input("Staff Throwing Load:", min_value=1, value=int(counts["Staff"].iloc[0]))
        update_counts = st.form_submit_button("Update TV")
        if update_counts:
            pd.DataFrame({"Grocery": [g_count], "Frozen": [f_count], "Staff": [staff_count]}).to_csv(COUNTS_FILE, index=False)
            st.rerun()

# --- MAIN DASHBOARD: THE TV SCREEN ---
@st.fragment(run_every="2s")
def live_tv_board():
    live_tasks = load_tasks()
    live_counts = load_counts()
    live_orders = load_orders()
    
    now = datetime.now()
    date_str = now.strftime("%A, %B %d, %Y")
    time_str = now.strftime("%I:%M %p")
    
    st.markdown("<h1 style='text-align: center; color: #ffffff; margin-bottom: 0px;'>Center Store Communication Board</h1>", unsafe_allow_html=True)
    st.markdown(f"<h3 style='text-align: center; color: #ff9800; margin-top: 0px;'>{date_str} | {time_str}</h3>", unsafe_allow_html=True)
    st.markdown("---")

    grocery_pieces = live_counts['Grocery'].iloc[0]
    frozen_pieces = live_counts['Frozen'].iloc[0]
    staff = live_counts['Staff'].iloc[0]
    total_pieces = grocery_pieces + frozen_pieces
    cases_per_hour_per_person = 60 
    
    if total_pieces > 0:
        total_rate = staff * cases_per_hour_per_person
        est_hours = round(total_pieces / total_rate, 1)
        est_text = f"Estimated Completion: {est_hours} hrs (with {staff} staff)"
    else:
        est_text = "No active load."

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"<div class='big-number' style='color: #4CAF50;'>Grocery: {grocery_pieces}</div>", unsafe_allow_html=True)
    with col2:
        st.markdown(f"<div class='big-number' style='color: #2196F3;'>Frozen: {frozen_pieces}</div>", unsafe_allow_html=True)
    
    st.markdown(f"<div class='est-time'>{est_text}</div>", unsafe_allow_html=True)
    st.markdown("---")
    
    # --- ZONE MAPPED TASKS ---
    open_tasks = live_tasks[live_tasks["Status"] == "Open"].copy()

    if open_tasks.empty:
        st.success("All caught up! The floor is clear.")
    else:
        # Sort tasks by Priority (Urgent -> High -> Routine) first
        priority_map = {"Urgent": 1, "High": 2, "Routine": 3}
        open_tasks["p_rank"] = open_tasks["Priority"].map(priority_map)
        open_tasks = open_tasks.sort_values(["p_rank", "Task_ID"])
        
        # Group visually by Zone
        active_zones = open_tasks["Zone"].unique()
        
        # Force a specific display order for zones so they don't jump around randomly
        display_order = ["General", "Front End", "Aisles 1-5", "Aisles 6-10", "Cooler/Freezer", "Backroom"]
        ordered_zones = [z for z in display_order if z in active_zones]
        # Catch any custom zones that might not be in the default list
        ordered_zones += [z for z in active_zones if z not in display_order]

        for current_zone in ordered_zones:
            st.markdown(f"<div class='zone-header'>📍 {current_zone}</div>", unsafe_allow_html=True)
            zone_tasks = open_tasks[open_tasks["Zone"] == current_zone]
            
            for _, row in zone_tasks.iterrows():
                t_id = row['Task_ID']
                p_class = f"task-{row['Priority'].lower()}"
                text_col, btn_col = st.columns([0.9, 0.1])
                with text_col:
                    st.markdown(f"<div class='{p_class}'>{row['Task_Detail']}</div>", unsafe_allow_html=True)
                with btn_col:
                    st.button("❌", key=f"del_task_{t_id}", on_click=delete_task, args=(t_id, active_user))

    st.markdown("---")

    # --- SPECIAL ORDERS ---
    st.subheader("📦 Incoming Special Orders")
    open_orders = live_orders[live_orders["Status"] == "Open"]

    if open_orders.empty:
        st.info("No special orders pending.")
    else:
        for _, row in open_orders.iterrows():
            o_id = row['Order_ID']
            text_col, btn_col = st.columns([0.9, 0.1])
            with text_col:
                st.markdown(f"<div class='order-box'>{row['Order_Detail']}</div>", unsafe_allow_html=True)
            with btn_col:
                st.button("❌", key=f"del_ord_{o_id}", on_click=delete_order, args=(o_id,))

# Turn the TV on
live_tv_board()