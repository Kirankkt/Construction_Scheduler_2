import os
import glob
import re
import time
import streamlit as st
import pandas as pd
from typing import Dict, Any, List

from data_ingestion import (
    parse_csv_to_tasks,
    load_drawing_notes_from_cache,
    rebuild_drawing_notes_cache,
    match_notes_to_tasks,
)
from scheduling import (
    compute_cpm_baseline,
    level_resources,
    compute_project_metrics,
    analyze_bottlenecks,
    suggest_capacities_to_hit_target,
)
from visualization import gantt_figure, critical_path_figure

st.set_page_config(page_title="Construction Scheduler", layout="wide")


# -------------------------
# Data file selection
# -------------------------
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")

def list_csvs(data_dir: str) -> List[str]:
    """All CSVs in data_dir, newest modified first."""
    return sorted(glob.glob(os.path.join(data_dir, "*.csv")), key=os.path.getmtime, reverse=True)

def parse_day_from_name(name: str):
    """Return integer DayN if present in filename, else None."""
    m = re.search(r"day\s*(\d+)", name, re.IGNORECASE)
    return int(m.group(1)) if m else None

def pick_latest_csv(files: List[str]) -> str | None:
    """Prefer highest DayN; fallback to newest modified."""
    if not files:
        return None
    scored = []
    for f in files:
        day = parse_day_from_name(os.path.basename(f))
        scored.append((f, day, os.path.getmtime(f)))
    scored.sort(key=lambda x: ((x[1] if x[1] is not None else -1), x[2]), reverse=True)
    return scored[0][0]

def file_sig(path: str) -> str:
    """Small signature to bust Streamlit cache when file changes."""
    st_ = os.stat(path)
    return f"{os.path.basename(path)}|{st_.st_size}|{int(st_.st_mtime)}"


# Bundled PDFs (drawing notes)
PDF_PATHS = [
    os.path.join(DATA_DIR, "ROY - CIVIL WORKS - DEMOLISION AND EXTENSION.pdf"),
    os.path.join(DATA_DIR, "ROY - CIVIL WORKS - FABRICATION.pdf"),
]


# -------------------------
# Sidebar controls
# -------------------------
with st.sidebar:
    st.header("Scenario Settings")
    hours_per_day = st.radio("Working hours per day", options=[7.0, 8.0], index=1, horizontal=True)
    start_date = st.date_input("Project start date", pd.to_datetime("today"))
    auto_chain = st.toggle("Auto-chain tasks within Section/Subsection by day order", value=True)
    pool_by_cat = st.toggle("Pool by category (ignore exact crew codes)", value=False)
    show_milestones = st.toggle("Show zero-duration tasks as milestones", value=True)
    target_days = st.number_input("Target duration (days)", min_value=1, value=30)
    enforce_target = st.toggle("Enforce target (advise to add capacity)", value=False)

    st.subheader("Data file")
    _all_csvs = list_csvs(DATA_DIR)
    auto_pick = st.toggle(
        "Auto-pick newest CSV",
        value=True,
        help="Chooses highest DayN in filename; if absent, the newest modified file."
    )
    if auto_pick:
        CSV_PATH = pick_latest_csv(_all_csvs) or os.path.join(DATA_DIR, "13 B Renovation_working.csv")
        st.caption(f"Auto-selected: {os.path.basename(CSV_PATH)}")
    else:
        if not _all_csvs:
            st.error("No CSVs found in /data.")
            st.stop()
        _names = [os.path.basename(p) for p in _all_csvs]
        _sel = st.selectbox("Choose CSV", _names, index=0)
        CSV_PATH = os.path.join(DATA_DIR, _sel)

    st.subheader("Drawings")
    use_notes = st.toggle("Use drawing notes", value=True)
    refresh_notes = st.button("Refresh notes from PDFs (parse/cache)")


# Verify files exist
missing = []
if not os.path.exists(CSV_PATH):
    missing.append(os.path.basename(CSV_PATH))
for p in PDF_PATHS:
    if not os.path.exists(p):
        missing.append(os.path.basename(p))
if missing:
    st.error("Missing bundled files: " + ", ".join(missing))
    st.stop()


# -------------------------
# Parse CSV (cached)
# -------------------------
@st.cache_data(show_spinner=False)
def _parse_csv_cached(path: str, hours_per_day: float, auto_chain: bool, sig: str):
    # 'sig' is an inert key to invalidate cache when file changes
    return parse_csv_to_tasks(path, working_hours_per_day=hours_per_day, auto_chain_within_subsection=auto_chain)

with st.spinner("Parsing CSV into tasks..."):
    tasks, warnings = _parse_csv_cached(CSV_PATH, hours_per_day, auto_chain, file_sig(CSV_PATH))

st.info(f"Using data file: **{os.path.basename(CSV_PATH)}**")

if warnings:
    for w in warnings:
        st.warning(w)
if not tasks:
    st.error("No tasks parsed from CSV.")
    st.stop()


# -------------------------
# Filters
# -------------------------
st.title("🔧 Construction Scheduling Optimizer")
st.caption("CPM + resource leveling; cached drawing notes on demand. No imputation of durations.")

st.subheader("Filters")

sections = sorted({t["section"] for t in tasks if t.get("section")})
subsections_all = sorted({t["subsection"] for t in tasks if t.get("subsection")})
categories_all = sorted({t["crew_category"] for t in tasks if t.get("crew_category")})
disciplines_all = sorted({t["discipline"] for t in tasks if t.get("discipline")})

sel_sections = st.multiselect("Sections", sections, default=sections or [])

subs_pool = sorted({t["subsection"] for t in tasks if t.get("subsection") and (not sel_sections or t["section"] in sel_sections)})
sel_subs = st.multiselect("Subsections", subs_pool, default=subs_pool or [])

sel_cats = st.multiselect("Crew categories", categories_all, default=categories_all or [])
sel_disc = st.multiselect("Discipline", disciplines_all, default=disciplines_all or [])

min_day = min((t["planned_day"] for t in tasks), default=1)
max_day = max((t["planned_day"] for t in tasks), default=1)
day_range = st.slider("Planned day range", min_value=int(min_day), max_value=int(max_day), value=(int(min_day), int(max_day)))

name_q = st.text_input("Task name contains", "").strip().lower()

def _passes(t):
    return (
        (not sel_sections or t["section"] in sel_sections)
        and (not sel_subs or t["subsection"] in sel_subs)
        and (not sel_cats or (t.get("crew_category") in sel_cats))
        and (not sel_disc or (t.get("discipline") in sel_disc))
        and (day_range[0] <= t["planned_day"] <= day_range[1])
        and (name_q in t["name"].lower())
    )

f_tasks = [t for t in tasks if _passes(t)]


# -------------------------
# Crew availability (categories)
# -------------------------
st.subheader("Crew Availability (by category)")
capacity_by_category: Dict[str, int] = {}
if categories_all:
    cols = st.columns(min(4, max(1, len(categories_all))))
    for i, cat in enumerate(categories_all):
        with cols[i % len(cols)]:
            capacity_by_category[cat] = st.number_input(
                f"Category {cat} crews", min_value=1, max_value=50, value=1, step=1, key=f"cap_{cat}"
            )
else:
    st.caption("No crew categories detected in CSV.")


# -------------------------
# CPM + Resource leveling
# -------------------------
with st.spinner("Computing CPM + leveled schedule..."):
    base = compute_cpm_baseline(f_tasks)
    schedule = level_resources(
        f_tasks, base,
        pool_by_category=pool_by_cat,
        capacity_by_category=(capacity_by_category if pool_by_cat else {})
    )

metrics = compute_project_metrics(schedule, hours_per_day=hours_per_day)
if enforce_target and metrics["duration_days"] > target_days:
    st.warning(
        f"Schedule exceeds target ({metrics['duration_days']:.1f} d > {target_days} d). "
        "Use Resources & Bottlenecks → What-if to get capacity suggestions."
    )


# -------------------------
# Tabs
# -------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Schedule", "Critical Path", "Resources & Bottlenecks", "Inefficiencies", "Notes"]
)

with tab1:
    st.subheader("Gantt")
    fig = gantt_figure(schedule, start_date=str(start_date), show_milestones=show_milestones)
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

with tab2:
    st.subheader("Baseline CPM")
    fig_cp = critical_path_figure(f_tasks, base, start_date=str(start_date))
    st.plotly_chart(fig_cp, use_container_width=True, theme="streamlit")

    crit_rows = []
    for t in f_tasks:
        info = base[t["id"]]
        if info.get("critical"):
            crit_rows.append({
                "Task ID": t["id"], "Section": t["section"], "Subsection": t["subsection"],
                "Name": t["name"], "ES": round(info["es"],1), "EF": round(info["ef"],1),
                "Slack (h)": round(info["slack"],1)
            })
    if crit_rows:
        st.dataframe(pd.DataFrame(crit_rows), hide_index=True, use_container_width=True)
    else:
        st.info("No strictly-zero-slack tasks found (or durations are missing).")

with tab3:
    st.subheader("Resource Utilization & Bottlenecks")
    delay_by_cat, idle_by_code = analyze_bottlenecks(f_tasks, base, schedule)
    if delay_by_cat:
        st.write("**Start delay vs CPM (hours) by crew category** — higher values indicate contention:")
        st.dataframe(
            pd.DataFrame(
                [{"Crew Category": k, "Total Start Delay (h)": round(v,1)} for k, v in sorted(delay_by_cat.items(), key=lambda x: -x[1])]
            ),
            hide_index=True, use_container_width=True
        )
    if idle_by_code:
        st.write("**Idle time by exact crew code (hours)** — gaps between tasks for the same crew:")
        st.dataframe(
            pd.DataFrame(
                [{"Crew Code": k, "Idle Time (h)": round(v,1)} for k, v in sorted(idle_by_code.items(), key=lambda x: -x[1])]
            ),
            hide_index=True, use_container_width=True
        )

    st.divider()
    st.subheader("What-if: hit target")
    st.caption("Greedy suggestion: adds capacity to the most constraining crew category (pooling required).")
    if st.button("Suggest capacities to meet target"):
        if not categories_all:
            st.info("No crew categories found in CSV to optimize.")
        else:
            caps, est_dur, steps = suggest_capacities_to_hit_target(
                f_tasks, base, hours_per_day, pool_by_cat, capacity_by_category, target_days
            )
            st.write("**Suggested category capacities**")
            st.json(caps)
            st.metric("Estimated duration with suggested caps (days)", f"{est_dur:.1f}")

with tab4:
    st.subheader("Inefficiencies & Data Gaps")
    missing = [t for t in f_tasks if (t["duration_hours"] is None)]
    st.write(f"Tasks with missing durations: **{len(missing)}** (kept as milestones; no imputation).")
    if missing:
        md = pd.DataFrame([
            {
                "Task ID": t["id"], "Section": t["section"], "Subsection": t["subsection"],
                "Discipline": t.get("discipline") or "",
                "Name": t["name"], "Planned Day": t["planned_day"],
                "Crew": t.get("crew_code") or t.get("crew_category") or ""
            }
            for t in missing
        ])
        st.dataframe(md, hide_index=True, use_container_width=True)

with tab5:
    st.subheader("Drawing Notes → Task suggestions")
    notes = []
    if use_notes:
        if refresh_notes:
            with st.spinner("Parsing PDFs and updating cache..."):
                notes = rebuild_drawing_notes_cache(PDF_PATHS, cache_dir=DATA_DIR)
        else:
            notes = load_drawing_notes_from_cache(cache_dir=DATA_DIR)
        if not notes and not refresh_notes:
            st.info("No cached notes yet. Click the refresh button in the sidebar once.")
        elif notes:
            matches = match_notes_to_tasks(notes, tasks, limit=3)
            nm_rows = []
            for rec in matches:
                for tid, tname, score in rec["matches"]:
                    nm_rows.append({"Note": rec["note"], "Match Task": tname, "Task ID": tid, "Score": score})
            st.dataframe(
                pd.DataFrame(nm_rows).sort_values(["Note","Score"], ascending=[True, False]),
                hide_index=True, use_container_width=True
            )

# Quick sanity summary of parsed hierarchy
with st.expander("Debug: Parsed hierarchy summary", expanded=False):
    from collections import defaultdict
    sec_summary = defaultdict(lambda: {"subsections": set(), "tasks": 0})
    for t in tasks:
        s = sec_summary[t["section"]]
        s["subsections"].add(t["subsection"])
        s["tasks"] += 1
    st.dataframe(
        pd.DataFrame([
            {"Section": k, "Subsections (#)": len(v["subsections"]), "Tasks": v["tasks"]}
            for k, v in sec_summary.items()
        ]).sort_values("Section"),
        hide_index=True, use_container_width=True
    )

st.divider()
st.subheader("Summary")
colA, colB = st.columns(2)
colA.metric("Estimated Duration (days)", f"{metrics['duration_days']:.1f}")
colB.metric("Tasks parsed", f"{len(f_tasks)}")
