import streamlit as st
import pandas as pd
import uuid
import hashlib
from datetime import datetime, timedelta, timezone
from sqlalchemy import text

# -------------------------
# TIMEZONE
# -------------------------
try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Edmonton")
except ImportError:
    import pytz
    LOCAL_TZ = pytz.timezone("America/Edmonton")

def now_local():
    return datetime.now(LOCAL_TZ)

def gen_id():
    return uuid.uuid4().hex

def hash_pin(pin):
    return hashlib.sha256(str(pin).encode()).hexdigest()

# -------------------------
# CONFIG
# -------------------------
st.set_page_config(page_title="TGP Comm Board", layout="wide")

TRUCK_DAYS = ["Sunday", "Tuesday", "Thursday"]
PREMIUM_STAFF = ["Chris", "Ashley", "Luke", "Chandler"]

# -------------------------
# DATABASE (POOLED)
# -------------------------
conn = st.connection("postgresql", type="sql", url=st.secrets["DB_URL"])

# -------------------------
# LOAD LAYER (FILTERED)
# -------------------------
@st.cache_data(ttl=2)
def load_raw():
    # ttl=0 bypasses the internal cache so the outer ttl=2 wrapper controls refresh
    return {
        "tasks": conn.query("SELECT * FROM tasks WHERE status='Open'", ttl=0),
        "oos": conn.query("SELECT * FROM oos WHERE status='Open'", ttl=0),
        "orders": conn.query("SELECT * FROM special_orders WHERE status='Open'", ttl=0),
        "expected": conn.query("SELECT * FROM expected_orders WHERE status='Pending'", ttl=0),
        "counts": conn.query("SELECT * FROM counts WHERE id=1", ttl=0),
        "staff": conn.query("SELECT * FROM staff WHERE active=1", ttl=0),
        "settings": conn.query("SELECT * FROM settings", ttl=0),
        "ticker": conn.query("SELECT * FROM ticker", ttl=0)
    }

def clear_cache():
    load_raw.clear()

# -------------------------
# COMPUTE LAYER
# -------------------------
def compute_vm(data):
    counts = data["counts"].fillna(0)
    settings = data["settings"]

    grocery = int(counts["grocery"].iloc[0]) if not counts.empty else 0
    frozen = int(counts["frozen"].iloc[0]) if not counts.empty else 0
    staff = max(1, int(counts["staff"].iloc[0])) if not counts.empty else 1

    now = now_local()
    today = now.strftime("%A")

    # SETTINGS
    cph = 55.0
    if not settings.empty:
        val = settings.loc[settings["setting_name"] == "Cases_Per_Hour", "setting_value"]
        if not val.empty:
            cph = max(1.0, float(val.iloc[0]))

    # TASK TIME
    tasks = data["tasks"]
    task_minutes = pd.to_numeric(tasks["est_mins"], errors="coerce").fillna(0).sum()

    # LOAD ONLY ON TRUCK DAYS
    load_cases = (grocery + frozen) if today in TRUCK_DAYS else 0

    load_hours = load_cases / cph
    task_hours = task_minutes / 60.0
    total_hours = (load_hours + task_hours) / staff

    eta = (
        (now + timedelta(hours=total_hours)).strftime("%I:%M %p")
        if total_hours > 0 else "N/A"
    )

    # Dynamically pull staff names if available, fallback to PREMIUM_STAFF
    staff_list = data["staff"]["name"].tolist() if not data["staff"].empty else PREMIUM_STAFF

    return {
        "now": now,
        "today": today,
        "grocery": grocery,
        "frozen": frozen,
        "staff_count": staff,
        "staff_list": staff_list,
        "tasks": tasks,
        "oos": data["oos"],
        "orders": data["orders"],
        "expected": data["expected"],
        "ticker": data["ticker"],
        "task_minutes": int(task_minutes),
        "total_hours": round(total_hours, 2),
        "eta": eta,
        "is_truck_day": today in TRUCK_DAYS
    }

# -------------------------
# WRITE LAYER (SAFE)
# -------------------------
def assign_task(task_id, widget_key):
    staff = st.session_state[widget_key]

    with conn.session as s:
        s.execute(
            text("UPDATE tasks SET assigned_to=:staff WHERE task_id=:task_id"),
            {"staff": staff, "task_id": task_id}
        )
        s.commit()

    clear_cache()

def complete_task(task_id, user):
    with conn.session as s:
        s.execute(
            text("""
                UPDATE tasks 
                SET status='Closed',
                    closed_by=:user,
                    time_closed=CURRENT_TIMESTAMP
                WHERE task_id=:task_id
            """),
            {"user": user, "task_id": task_id}
        )
        s.commit()

    clear_cache()

def complete_oos(oos_id, user):
    with conn.session as s:
        s.execute(
            text("""
                UPDATE oos 
                SET status='Closed',
                    closed_by=:user,
                    time_closed=CURRENT_TIMESTAMP
                WHERE oos_id=:oos_id
            """),
            {"user": user, "oos_id": oos_id}
        )
        s.commit()

    clear_cache()

# -------------------------
# RENDER LAYER
# -------------------------
def render(vm, active_user, is_tv=False):
    now = vm["now"]

    st.markdown(f"""
    ### TGP CENTRE STORE — {vm['today']}
    {now.strftime('%b %d %Y %I:%M %p')}
    """)

    st.markdown(f"""
    **Grocery:** {vm['grocery']}  |  **Frozen:** {vm['frozen']}  |  **Staff:** {vm['staff_count']}  |  **Tasks:** {vm['task_minutes']} mins  |  **Truck Day:** {"YES" if vm['is_truck_day'] else "NO"}  |  **ETA:** {vm['eta']}
    """)

    st.divider()

    # TASKS
    st.subheader("Tasks")
    for _, r in vm["tasks"].iterrows():
        key_name = f"a_{r['task_id']}"

        if is_tv:
            # Display assigned staff visually on TV mode
            st.write(f"[{r['zone']}] {r['task_detail']} ({r['est_mins']}m) — **{r.get('assigned_to', 'Unassigned')}**")
        else:
            c1, c2, c3 = st.columns([5, 3, 2])

            c1.write(f"[{r['zone']}] {r['task_detail']} ({r['est_mins']}m)")

            options = ["Unassigned"] + vm["staff_list"]
            current = r.get("assigned_to", "Unassigned")

            if current not in options:
                options.append(current)

            c2.selectbox(
                "Assign",
                options,
                index=options.index(current),
                key=key_name,
                on_change=assign_task,
                args=(r["task_id"], key_name),
                label_visibility="collapsed"
            )

            c3.button(
                "Done",
                key=f"done_{r['task_id']}",
                on_click=complete_task,
                args=(r["task_id"], active_user)
            )

    # OOS
    st.subheader("Shelf Holes")
    for _, r in vm["oos"].iterrows():
        if is_tv:
            st.write(f"{r['zone']}: {r['hole_count']}")
        else:
            c1, c2 = st.columns([4, 1])
            c1.write(f"{r['zone']}: {r['hole_count']}")
            c2.button(
                "CLR",
                key=f"clr_{r['oos_id']}",
                on_click=complete_oos,
                args=(r["oos_id"], active_user)
            )

    # ORDERS
    st.subheader("Orders")
    for _, r in vm["orders"].iterrows():
        st.write(f"{r['item']} - {r['customer']}")

    # EXPECTED
    st.subheader("Inbound")
    for _, r in vm["expected"].iterrows():
        st.write(f"🚚 {r['vendor']}")

    # TICKER
    if not vm["ticker"].empty:
        msgs = " | ".join(vm["ticker"]["message"].tolist())
        st.info(msgs)

# -------------------------
# ENTRYPOINT & TV MODE
# -------------------------
active_user = st.sidebar.selectbox("Operator", PREMIUM_STAFF)
tv_mode = st.sidebar.toggle("TV Mode")

if tv_mode:
    @st.fragment(run_every=4)
    def tv_loop():
        # Data fetching is inside the fragment, pulling a fresh copy from cache/DB every 4s
        data = load_raw()
        vm = compute_vm(data)
        render(vm, active_user, is_tv=True)
        
    tv_loop()
else:
    # Standard interactive mode executes once per user action
    data = load_raw()
    vm = compute_vm(data)
    render(vm, active_user, is_tv=False)
