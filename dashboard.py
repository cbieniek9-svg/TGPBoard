import streamlit as st
import pandas as pd
import os
import urllib.parse
from datetime import datetime, timedelta, timezone

# --- PAGE CONFIGURATION ---
# Set sidebar to collapsed by default so it stays out of the way on the TV
st.set_page_config(page_title="TGP Operations OS", layout="wide", initial_sidebar_state="collapsed")

# --- TV STYLING ---
st.markdown("""
    <style>
    /* HIDE STREAMLIT UI & CRUSH MARGINS TO PREVENT SCROLLING */
    header { visibility: hidden; }
    footer { visibility: hidden; }
    #MainMenu { visibility: hidden; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; padding-left: 2rem; padding-right: 2rem; max-width: 100%; }

    .stApp { background-color: #0d1117; }
    
    .task-card { font-size: 24px; padding: 15px; border-radius: 8px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; color: #ffffff; background-color: #161b22; border: 1px solid #30363d;}
    .priority-routine { border-left: 6px solid #2ea043; }
    .priority-high { border-left: 6px solid #ff7b72; }
    .priority-urgent { border-left: 6px solid #f85149; background-color: #3b0000; font-weight: bold;}
    
    .oos-card { font-size: 20px; padding: 12px; background-color: #1c2128; border-left: 4px solid #f85149; border-radius: 4px; margin-bottom: 8px; color: #ffffff; }
    .order-card { font-size: 20px; padding: 12px; background-color: #1c2128; border-left: 4px solid #a371f7; border-radius: 4px; margin-bottom: 8px; color: #ffffff; }
    
    .widget-box { background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 15px; text-align: center; margin-bottom: 15px; }
    .widget-title { color: #8b949e; font-size: 16px; text-transform: uppercase; letter-spacing: 1px; }
    .widget-value { color: #c9d1d9; font-size: 36px; font-weight: 800; line-height: 1.2; }
    
    .zone-header { color: #a5d6ff; font-size: 24px; margin-top: 20px; margin-bottom: 10px; border-bottom: 1px solid #21262d; padding-bottom: 5px; font-weight: 600; text-transform: uppercase;}
    
    div[data-testid="stButton"] > button { height: 50px; width: 100%; font-size: 18px; font-weight: 600; border-radius: 8px;}
    </style>
""", unsafe_allow_html=True)

# --- DATA MANAGEMENT ---
TASKS_FILE = "tasks.csv"
COUNTS_FILE = "counts.csv"
ORDERS_FILE = "orders.csv"
TICKER_FILE = "ticker.csv"
WALKS_FILE = "walks.csv"
OOS_FILE = "oos.csv"

def get_yeg_now():
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=6)

# Initialize Files safely
for f, cols in {
    TASKS_FILE: ["Task_ID", "Task_Detail", "Status", "Priority", "Zone", "Submitted_By", "Time_Submitted", "Closed_By", "Time_Closed"], 
    ORDERS_FILE: ["Order_ID", "Order_Detail", "Status"], 
    TICKER_FILE: ["Message_ID", "Message", "Type"],
    WALKS_FILE: ["Walk_ID", "Completed_By", "Time_Completed"],
    OOS_FILE: ["OOS_ID", "Item", "Zone", "Status", "Logged_By", "Time_Logged"],
    COUNTS_FILE: ["Grocery", "Frozen", "Staff"]
}.items():
    if not os.path.exists(f):
        pd.DataFrame(columns=cols).to_csv(f, index=False)
        if f == COUNTS_FILE: 
            pd.DataFrame({"Grocery": [0], "Frozen": [0], "Staff": [1]}).to_csv(f, index=False)

def load_data(file): return pd.read_csv(file)
def save_data(df, file): df.to_csv(file, index=False)

# --- ACTIONS ---
def delete_task(tid, user):
    df = load_data(TASKS_FILE)
    df.loc[df["Task_ID"] == tid, ["Status", "Closed_By", "Time_Closed"]] = ["Closed", user, get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")]
    save_data(df, TASKS_FILE)

def delete_order(oid):
    df = load_data(ORDERS_FILE)
    df.loc[df["Order_ID"] == oid, "Status"] = "Closed"
    save_data(df, ORDERS_FILE)

def delete_oos(oid):
    df = load_data(OOS_FILE)
    df.loc[df["OOS_ID"] == oid, "Status"] = "Closed"
    save_data(df, OOS_FILE)

# --- SIDEBAR (MOBILE UI) ---
tasks, counts, orders, ticker, walks, oos = [load_data(f) for f in [TASKS_FILE, COUNTS_FILE, ORDERS_FILE, TICKER_FILE, WALKS_FILE, OOS_FILE]]
zones_list = ["General", "Aisle 1", "Aisle 2", "Aisle 3", "Aisle 4", "Aisle 5", "Aisle 6", "Aisle 7", "Aisle 8", "Receiving", "Freezer", "Click and Collect", "Bakery", "Outside"]

with st.sidebar:
    st.header("📱 Control Panel")
    active_user = st.selectbox("👤 User:", ["Chris", "Ashley", "Luke", "Chandler"])
    
    # NAVIGATION TABS
    tab_tasks, tab_oos, tab_labor, tab_admin = st.tabs(["🎯 Tasks", "🚫 OOS", "🚚 Labor", "⚙️ Admin"])
    
    with tab_tasks:
        if st.button(f"🚶 Log Perimeter Walk"):
            new_walk = pd.DataFrame([{"Walk_ID": 1 if walks.empty else walks["Walk_ID"].max() + 1, "Completed_By": active_user, "Time_Completed": get_yeg_now().strftime("%I:%M %p")}])
            save_data(pd.concat([walks, new_walk], ignore_index=True), WALKS_FILE)
            st.rerun()

        with st.form("add_task", clear_on_submit=True):
            st.markdown("#### Assign Task")
            p = st.selectbox("Priority:", ["Routine", "High", "Urgent"])
            z = st.selectbox("Zone:", zones_list)
            d = st.text_area("Detail:")
            if st.form_submit_button("Push Task") and d.strip():
                new_t = pd.DataFrame([{"Task_ID": 1 if tasks.empty else tasks["Task_ID"].max() + 1, "Task_Detail": d, "Status": "Open", "Priority": p, "Zone": z, "Submitted_By": active_user, "Time_Submitted": get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")}])
                save_data(pd.concat([tasks, new_t], ignore_index=True), TASKS_FILE)
                st.rerun()

        with st.form("add_order_form", clear_on_submit=True):
            st.markdown("#### Special Order")
            order_desc = st.text_input("Item/Customer:")
            if st.form_submit_button("Push Order") and order_desc.strip():
                new_o = pd.DataFrame([{"Order_ID": 1 if orders.empty else orders["Order_ID"].max() + 1, "Order_Detail": order_desc.strip(), "Status": "Open"}])
                save_data(pd.concat([orders, new_o], ignore_index=True), ORDERS_FILE)
                st.rerun()

    with tab_oos:
        st.markdown("#### Log Out of Stock")
        with st.form("add_oos", clear_on_submit=True):
            item = st.text_input("Item Name / UPC:")
            izone = st.selectbox("Location:", zones_list, index=1)
            if st.form_submit_button("Log OOS") and item.strip():
                new_oos = pd.DataFrame([{"OOS_ID": 1 if oos.empty else oos["OOS_ID"].max() + 1, "Item": item.strip(), "Zone": izone, "Status": "Open", "Logged_By": active_user, "Time_Logged": get_yeg_now().strftime("%Y-%m-%d %H:%M:%S")}])
                save_data(pd.concat([oos, new_oos], ignore_index=True), OOS_FILE)
                st.rerun()
        
        st.divider()
        st.markdown("#### 📬 Email OOS Report")
        target_email = st.text_input("Send to:", value="manager@tgp.com")
        if st.button("Generate Email Link"):
            open_oos = oos[oos["Status"] == "Open"]
            if open_oos.empty:
                st.warning("No open OOS items to send.")
            else:
                body = f"Center Store OOS Report - {get_yeg_now().strftime('%Y-%m-%d')}\n\n"
                for _, r in open_oos.iterrows():
                    body += f"- {r['Item']} ({r['Zone']})\n"
                
                subject = urllib.parse.quote(f"TGP OOS Report: {get_yeg_now().strftime('%Y-%m-%d')}")
                encoded_body = urllib.parse.quote(body)
                mailto_link = f"mailto:{target_email}?subject={subject}&body={encoded_body}"
                
                st.markdown(f"""
                    <a href="{mailto_link}" target="_blank" style="text-decoration:none;">
                        <button style="width:100%; padding:10px; background-color:#58a6ff; color:white; border:none; border-radius:5px; font-weight:bold; font-size:16px; cursor:pointer;">
                            TAP TO OPEN EMAIL APP
                        </button>
                    </a>
                """, unsafe_allow_html=True)

    with tab_labor:
        st.markdown("#### Calculate Labor")
        with st.form("truck_info"):
            g = st.number_input("Grocery Pieces:", value=int(counts["Grocery"].iloc[0]))
            f = st.number_input("Frozen Pieces:", value=int(counts["Frozen"].iloc[0]))
            s = st.number_input("Staff on Floor:", min_value=1, value=int(counts["Staff"].iloc[0]))
            if st.form_submit_button("Update Engine"):
                pd.DataFrame({"Grocery": [g], "Frozen": [f], "Staff": [s]}).to_csv(COUNTS_FILE, index=False)
                st.rerun()

    with tab_admin:
        pin = st.text_input("Admin PIN:", type="password")
        if pin == "0000":
            with st.form("add_ticker_form", clear_on_submit=True):
                st.markdown("#### 📣 Push Broadcast")
                msg_type = st.selectbox("Type:", ["Kudos 🌟", "Alert 📢"])
                msg_text = st.text_input("Message:")
                if st.form_submit_button("Send to Ticker") and msg_text.strip():
                    new_m_id = 1 if ticker.empty else ticker["Message_ID"].max() + 1
                    save_data(pd.concat([ticker, pd.DataFrame([{"Message_ID": new_m_id, "Message": msg_text.strip(), "Type": msg_type}])], ignore_index=True), TICKER_FILE)
                    st.rerun()
                    
            if st.button("🚨 NUKE BOARD"):
                for f in [TASKS_FILE, ORDERS_FILE, TICKER_FILE, WALKS_FILE, OOS_FILE]:
                    pd.read_csv(f).iloc[0:0].to_csv(f, index=False)
                st.rerun()


# --- MAIN DASHBOARD (TV UI) ---
# The @st.fragment decorator silently re-runs this specific function every 10 seconds.
@st.fragment(run_every=10)
def live_tv_board():
    t, c, o, tk, w, out_stock = [load_data(f) for f in [TASKS_FILE, COUNTS_FILE, ORDERS_FILE, TICKER_FILE, WALKS_FILE, OOS_FILE]]
    now = get_yeg_now()
    
    st.markdown(f"""
        <h1 style='text-align: center; color: #ffffff; font-weight: 900; margin-bottom: -10px;'>
            CENTER STORE OPERATIONS
        </h1>
        <h4 style='text-align: center; color: #8b949e; margin-bottom: 20px;'>
            {now.strftime('%A, %B %d | %I:%M %p')}
        </h4>
    """, unsafe_allow_html=True)
    
    # --- LABOR ENGINE MATH ---
    g_count = int(c['Grocery'].iloc[0])
    f_count = int(c['Frozen'].iloc[0])
    staff_count = int(c['Staff'].iloc[0])
    
    req_hours = (g_count + f_count) / 55.0  # Target: 55 cases/hour
    avail_hours = staff_count * 7.5
    variance = avail_hours - req_hours
    
    if (g_count + f_count) == 0:
        lab_stat = "No Load"
        lab_color = "#8b949e"
    elif variance >= 0:
        lab_stat = f"Surplus: +{round(variance, 1)}h"
        lab_color = "#2ea043"
    else:
        lab_stat = f"Deficit: {round(variance, 1)}h"
        lab_color = "#f85149"

    # --- TOP METRICS ROW ---
    m1, m2, m3, m4, m5 = st.columns(5)
    last_w = "Pending" if w.empty else f"{w.iloc[-1]['Time_Completed']} ({w.iloc[-1]['Completed_By']})"

    m1.markdown(f"""
        <div class='widget-box'>
            <div class='widget-title'>Grocery Pieces</div>
            <div class='widget-value'>{g_count}</div>
        </div>
    """, unsafe_allow_html=True)
    
    m2.markdown(f"""
        <div class='widget-box'>
            <div class='widget-title'>Frozen Pieces</div>
            <div class='widget-value'>{f_count}</div>
        </div>
    """, unsafe_allow_html=True)
    
    m3.markdown(f"""
        <div class='widget-box'>
            <div class='widget-title'>Floor Staff</div>
            <div class='widget-value'>{staff_count}</div>
        </div>
    """, unsafe_allow_html=True)
    
    m4.markdown(f"""
        <div class='widget-box'>
            <div class='widget-title'>Labor Engine</div>
            <div class='widget-value' style='color: {lab_color};'>{lab_stat}</div>
        </div>
    """, unsafe_allow_html=True)
    
    m5.markdown(f"""
        <div class='widget-box'>
            <div class='widget-title'>Last Walk</div>
            <div class='widget-value' style='font-size:26px; padding-top:8px;'>{last_w}</div>
        </div>
    """, unsafe_allow_html=True)

    # --- MAIN CONTENT SPLIT ---
    col_tasks, col_oos = st.columns([0.65, 0.35])
    
    with col_tasks:
        st.markdown("<div class='zone-header'>📍 ACTIVE TASKS</div>", unsafe_allow_html=True)
        open_t = t[t["Status"] == "Open"].copy()
        if open_t.empty:
            st.success("✅ Floor is clear.")
        else:
            open_t["p_rank"] = open_t["Priority"].map({"Urgent": 1, "High": 2, "Routine": 3})
            open_t = open_t.sort_values(["p_rank", "Task_ID"])
            for z in [z for z in zones_list if z in open_t["Zone"].unique()]:
                st.write(f"**{z}**")
                for _, row in open_t[open_t["Zone"] == z].iterrows():
                    cols = st.columns([0.85, 0.15])
                    cols[0].markdown(f"""
                        <div class='task-card priority-{row['Priority'].lower()}'>
                            {row['Task_Detail']}
                        </div>
                    """, unsafe_allow_html=True)
                    cols[1].button("❌", key=f"t_{row['Task_ID']}", on_click=delete_task, args=(row['Task_ID'], active_user))

    with col_oos:
        st.markdown("<div class='zone-header'>🚫 OUT OF STOCK ALERTS</div>", unsafe_allow_html=True)
        open_oos = out_stock[out_stock["Status"] == "Open"]
        if open_oos.empty:
            st.info("No OOS items logged.")
        else:
            for _, r in open_oos.iterrows():
                cols = st.columns([0.8, 0.2])
                cols[0].markdown(f"""
                    <div class='oos-card'>
                        <strong>{r['Item']}</strong><br>
                        <span style='color:#8b949e; font-size:16px;'>{r['Zone']} | By: {r['Logged_By']}</span>
                    </div>
                """, unsafe_allow_html=True)
                cols[1].button("✅", key=f"oos_{r['OOS_ID']}", on_click=delete_oos, args=(r['OOS_ID'],))
        
        st.markdown("<div class='zone-header' style='margin-top:40px;'>📦 SPECIAL ORDERS</div>", unsafe_allow_html=True)
        open_o = o[o["Status"] == "Open"]
        if open_o.empty:
            st.info("No special orders.")
        else:
            for _, r in open_o.iterrows():
                cols = st.columns([0.8, 0.2])
                cols[0].markdown(f"""
                    <div class='order-card'>
                        {r['Order_Detail']}
                    </div>
                """, unsafe_allow_html=True)
                cols[1].button("✅", key=f"o_{r['Order_ID']}", on_click=delete_order, args=(r['Order_ID'],))

    # --- TICKER ---
    if not tk.empty:
        t_str = "".join([f"<span style='color: {'#FFD700' if 'Kudos' in r['Type'] else '#ff7b72'};'>{'🌟' if 'Kudos' in r['Type'] else '📢'} {r['Message']}</span> &nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp; " for _, r in tk.iterrows()])
        st.markdown(f"""
            <div style='background:#010409; padding:15px; border-top:2px solid #58a6ff;'>
                <marquee style='font-size:30px; font-weight:bold;'>{t_str}</marquee>
            </div>
        """, unsafe_allow_html=True)

live_tv_board()
