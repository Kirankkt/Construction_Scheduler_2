
from typing import Dict, Any, Optional, List
import pandas as pd
import plotly.express as px

def _hours_to_datetime(base_date: pd.Timestamp, hours: float) -> pd.Timestamp:
    return base_date + pd.to_timedelta(hours, unit="h")

def gantt_figure(schedule: Dict[str, Dict[str, Any]], start_date: Optional[str], show_milestones: bool):
    if not schedule:
        return px.timeline(pd.DataFrame(columns=["Task","Start","Finish"]), x_start="Start", x_end="Finish", y="Task")
    base = pd.to_datetime(start_date) if start_date else pd.to_datetime("2025-01-01")
    rows = []
    for tid, s in schedule.items():
        dur = float(s.get("duration") or 0.0)
        adj_finish = s["finish"]
        if dur == 0.0 and show_milestones:
            adj_finish = s["start"] + 0.01
        rows.append({
            "Task ID": tid,
            "Task": f"{s['task']} ({s.get('subsection')})",
            "Section": s.get("section") or "N/A",
            "Crew": s.get("crew_code") or (s.get("crew_category") or "N/A"),
            "Start": _hours_to_datetime(base, s["start"]),
            "Finish": _hours_to_datetime(base, adj_finish),
            "Duration (h)": dur
        })
    df = pd.DataFrame(rows)
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task", color="Section",
                      hover_data=["Task ID", "Crew", "Duration (h)", "Section"])
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(title="Project Schedule (Gantt)", xaxis_title="Date/Time", yaxis_title="Tasks")
    return fig

def critical_path_figure(tasks: List[Dict[str, Any]], base_info: Dict[str, Dict[str, float]], start_date: Optional[str]):
    if not tasks:
        return px.timeline(pd.DataFrame(columns=["Task","Start","Finish"]), x_start="Start", x_end="Finish", y="Task")
    base = pd.to_datetime(start_date) if start_date else pd.to_datetime("2025-01-01")
    rows = []
    for t in tasks:
        info = base_info[t["id"]]
        rows.append({
            "Task ID": t["id"],
            "Task": f"{t['name']} ({t.get('subsection')})",
            "Critical": "Yes" if info.get("critical") else "No",
            "Start": _hours_to_datetime(base, info["es"]),
            "Finish": _hours_to_datetime(base, info["ef"]),
            "Slack (h)": info.get("slack", 0.0),
            "Section": t.get("section") or "N/A"
        })
    df = pd.DataFrame(rows)
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task", color="Critical",
                      hover_data=["Task ID", "Slack (h)", "Section"])
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(title="Baseline CPM (Critical tasks in red)", xaxis_title="Date/Time", yaxis_title="Tasks")
    return fig
