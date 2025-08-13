
import os
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

st.title("🔧 Construction Scheduling Optimizer")
st.caption("CPM + resource leveling; cached drawing notes on demand.")

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "13 B Renovation_working.csv")
PDF_PATHS = [
    os.path.join(DATA_DIR, "ROY - CIVIL WORKS - DEMOLISION AND EXTENSION.pdf"),
    os.path.join(DATA_DIR, "ROY - CIVIL WORKS - FABRICATION.pdf"),
]

missing = [p for p in [CSV_PATH] + PDF_PATHS if not os.path.exists(p)]
if missing:
    st.error("Missing bundled files: " + ", ".join([os.path.basename(p) for p in missing]))
    st.stop()

with st.sidebar:
    st.header("Scenario Settings")
    hours_per_day = st.radio("Working hours per day", options=[7.0, 8.0], index=1, horizontal=True)
    start_date = st.date_input("Project start date", pd.to_datetime("today"))
    auto_chain = st.toggle("Auto-chain tasks within Section/Subsection by day order", value=True)
    pool_by_cat = st.toggle("Pool by category (ignore exact crew codes)", value=False)
    show_milestones = st.toggle("Show zero-duration tasks as milestones", value=True)
    target_days = st.number_input("Target duration (days)", min_value=1, value=30)
    enforce_target = st.toggle("Enforce target (advise to add capacity)", value=False)

    st.subheader("Drawings")
    use_notes = st.toggle("Use drawing notes", value=True)
    refresh_notes = st.button("Refresh notes from PDFs (parse/cache)")

@st.cache_data(show_spinner=False)
def _parse_csv_cached(path: str, hours_per_day: float, auto_chain: bool):
    return parse_csv_to_tasks(path, working_hours_per_day=hours_per_day, auto_chain_within_subsection=auto_chain)

with st.spinner("Parsing CSV into tasks..."):
    tasks, warnings = _parse_csv_cached(CSV_PATH, hours_per_day, auto_chain)

if warnings:
    for w in warnings:
        st.warning(w)
if not tasks:
    st.error("No tasks parsed from CSV.")
    st.stop()

sections = sorted({t["section"] for t in tasks if t.get("section")})
crew_cats = sorted({t["crew_category"] for t in tasks if t.get("crew_category")})

st.subheader("Filters")
sel_sections = st.multiselect("Sections", sections, default=sections)
f_tasks = [t for t in tasks if (not sel_sections or (t.get("section") in sel_sections))]

with st.spinner("Computing CPM + leveled schedule..."):
    base = compute_cpm_baseline(f_tasks)
    schedule = level_resources(f_tasks, base, pool_by_category=pool_by_cat, capacity_by_category={c:1 for c in crew_cats})

metrics = compute_project_metrics(schedule, hours_per_day=hours_per_day)
if enforce_target and metrics["duration_days"] > target_days:
    st.warning(
        f"Schedule exceeds target ({metrics['duration_days']:.1f} d > {target_days} d). "
        "Use 'Resources & Bottlenecks → What‑if' to get capacity suggestions."
    )

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Schedule", "Critical Path", "Resources & Bottlenecks", "Inefficiencies", "Notes"])

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
        st.info("No strictly-zero-slack tasks found (all tasks have some float or durations are missing).")

with tab3:
    st.subheader("Resource Utilization & Bottlenecks")
    from scheduling import analyze_bottlenecks, suggest_capacities_to_hit_target
    delay_by_cat, idle_by_code = analyze_bottlenecks(f_tasks, base, schedule)
    if delay_by_cat:
        st.write("**Start delay vs CPM (hours) by crew category** — higher values indicate contention:")
        st.dataframe(pd.DataFrame([{"Crew Category": k, "Total Start Delay (h)": round(v,1)} for k, v in sorted(delay_by_cat.items(), key=lambda x: -x[1])]), hide_index=True, use_container_width=True)
    if idle_by_code:
        st.write("**Idle time by exact crew code (hours)** — gaps between tasks for the same crew:")
        st.dataframe(pd.DataFrame([{"Crew Code": k, "Idle Time (h)": round(v,1)} for k, v in sorted(idle_by_code.items(), key=lambda x: -x[1])]), hide_index=True, use_container_width=True)
    st.divider()
    st.subheader("What‑if: hit target")
    st.caption("Greedy suggestion: adds capacity to the most constraining crew category (pooling required).")
    if st.button("Suggest capacities to meet target"):
        if not crew_cats:
            st.info("No crew categories found in CSV to optimize.")
        else:
            caps, est_dur, steps = suggest_capacities_to_hit_target(f_tasks, base, hours_per_day, pool_by_cat, {c:1 for c in crew_cats}, target_days)
            st.write("**Suggested category capacities**")
            st.json(caps)
            st.metric("Estimated duration with suggested caps (days)", f"{est_dur:.1f}")

with tab4:
    st.subheader("Inefficiencies & Data Gaps")
    missing = [t for t in f_tasks if (t["duration_hours"] is None)]
    st.write(f"Tasks with missing durations: **{len(missing)}** (kept as milestones; no imputation).")
    if missing:
        md = pd.DataFrame([
            {"Task ID": t["id"], "Section": t["section"], "Subsection": t["subsection"], "Name": t["name"], "Planned Day": t["planned_day"], "Crew": t.get("crew_code") or t.get("crew_category") or ""}
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
            import pandas as pd
            st.dataframe(pd.DataFrame(nm_rows).sort_values(["Note","Score"], ascending=[True, False]), hide_index=True, use_container_width=True)

st.divider()
st.subheader("Summary")
colA, colB = st.columns(2)
colA.metric("Estimated Duration (days)", f"{metrics['duration_days']:.1f}")
colB.metric("Tasks parsed", f"{len(f_tasks)}")
