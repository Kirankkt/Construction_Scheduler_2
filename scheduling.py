from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, deque
import math

def topological_order(
    tasks: List[Dict[str, Any]],
    deps: Optional[Dict[str, set]] = None
) -> List[str]:
    """
    Return task IDs in dependency-respecting order.
    If deps is provided, it must already be filtered to contain only IDs present in tasks.
    """
    ids = [t["id"] for t in tasks]
    id_set = set(ids)

    if deps is None:
        # Filter dependencies to within the current id_set
        deps = {t["id"]: set(d for d in t.get("dependencies", []) if d in id_set) for t in tasks}

    indeg = {tid: len(deps.get(tid, set())) for tid in ids}
    q = deque([tid for tid, d in indeg.items() if d == 0])

    order: List[str] = []
    # Work on a local copy so we can mutate safely
    deps_local: Dict[str, set] = {k: set(v) for k, v in deps.items()}

    while q:
        u = q.popleft()
        order.append(u)
        for v in ids:
            dv = deps_local.get(v, set())
            if u in dv:
                dv.remove(u)
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)

    # If there’s a cycle or unresolved deps, append remaining to keep going
    remaining = [tid for tid in ids if tid not in order]
    return order + remaining

def compute_cpm_baseline(tasks: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """
    Compute an ASAP CPM baseline ignoring resources.
    Missing durations are treated as 0 (marker tasks).
    Dependencies that reference tasks outside the provided list are ignored.
    Returns dict task_id -> {es, ef, ls, lf, duration, slack, critical}
    """
    ids = [t["id"] for t in tasks]
    id_set = set(ids)

    # Filter deps to those inside the filtered task set
    deps = {t["id"]: set(d for d in t.get("dependencies", []) if d in id_set) for t in tasks}

    info = {t["id"]: {"duration": float(t["duration_hours"] or 0.0)} for t in tasks}
    order = topological_order(tasks, deps)

    # forward pass (ES/EF)
    for tid in order:
        # consider only in-set predecessors
        preds = deps.get(tid, set())
        es = 0.0
        if preds:
            # All preds are guaranteed in-set now
            es = max(info[p]["ef"] for p in preds)
        ef = es + info[tid]["duration"]
        info[tid]["es"] = es
        info[tid]["ef"] = ef

    # project finish
    proj_finish = max((info[tid]["ef"] for tid in info), default=0.0)

    # successors map (within the set)
    succs = {tid: set() for tid in ids}
    for v in ids:
        for d in deps.get(v, set()):
            succs[d].add(v)

    # backward pass (LS/LF)
    for tid in reversed(order):
        if not succs[tid]:
            lf = proj_finish
        else:
            lf = min(info[s]["ls"] for s in succs[tid])
        ls = lf - info[tid]["duration"]
        info[tid]["ls"] = ls
        info[tid]["lf"] = lf
        info[tid]["slack"] = max(0.0, ls - info[tid]["es"])
        info[tid]["critical"] = (abs(info[tid]["slack"]) < 1e-9)

    return info

def level_resources(tasks: List[Dict[str, Any]],
                    base_info: Dict[str, Dict[str, float]],
                    pool_by_category: bool,
                    capacity_by_category: Dict[str, int]) -> Dict[str, Dict[str, float]]:
    """
    Apply simple resource leveling.
    If pool_by_category=True: limit concurrent tasks by crew_category capacity (e.g., '2' => 2 crews max).
    Else: respect exact crew_code: each code is exclusive (capacity=1).
    Dependencies that are outside the provided task list are ignored.
    Returns schedule dict: task_id -> {start, finish, duration, delay_vs_cpm_start, delay_vs_cpm_finish}
    """
    tasks_by_id = {t["id"]: t for t in tasks}
    id_set = set(tasks_by_id.keys())
    # sanitize deps to only in-set IDs for this phase as well
    deps = {tid: [d for d in tasks_by_id[tid].get("dependencies", []) if d in id_set] for tid in id_set}

    order = sorted(tasks, key=lambda t: (base_info[t["id"]]["es"], t["planned_day"], t["name"]))

    if pool_by_category:
        scheduled = []  # list of {"cat","start","finish"}
    else:
        from collections import defaultdict
        code_busy_until = defaultdict(float)  # crew_code -> time

    schedule: Dict[str, Dict[str, float]] = {}
    for t in order:
        tid = t["id"]
        dur = float(t["duration_hours"] or 0.0)

        # dependency-ready time: only consider predecessors inside the current filtered set
        est = 0.0
        for dep in deps.get(tid, []):
            if dep in schedule:
                est = max(est, schedule[dep]["finish"])
            elif dep in base_info:
                est = max(est, base_info[dep]["ef"])
            # else: dep is outside; ignore

        # also don't start before CPM ES (standard resource leveling practice)
        est = max(est, base_info[tid]["es"])

        if not t.get("crew_category") and not t.get("crew_code"):
            start = est
        else:
            if pool_by_category:
                cat = t.get("crew_category") or "UNSPEC"
                cap = max(1, int(capacity_by_category.get(cat, 1)))
                start = est

                def cat_active_at(tp: float) -> int:
                    return sum(1 for rec in scheduled if rec["cat"] == cat and rec["start"] < tp < rec["finish"])

                while dur > 0 and cat_active_at(start) >= cap:
                    finishes = [rec["finish"] for rec in scheduled if rec["cat"] == cat and rec["finish"] > start]
                    start = min(finishes) if finishes else start
            else:
                code = t.get("crew_code") or "UNSPEC"
                ready = code_busy_until[code]
                start = max(est, ready)

        finish = start + dur
        schedule[tid] = {
            "task": t["name"],
            "section": t.get("section"),
            "subsection": t.get("subsection"),
            "start": start,
            "finish": finish,
            "duration": dur,
            "crew_code": t.get("crew_code"),
            "crew_category": t.get("crew_category"),
            "delay_vs_cpm_start": max(0.0, start - base_info[tid]["es"]),
            "delay_vs_cpm_finish": max(0.0, finish - base_info[tid]["ef"]),
        }
        if pool_by_category:
            scheduled.append({"cat": t.get("crew_category") or "UNSPEC", "start": start, "finish": finish})
        else:
            code = t.get("crew_code") or "UNSPEC"
            code_busy_until[code] = finish

    return schedule

def compute_project_metrics(schedule: Dict[str, Dict[str, float]], hours_per_day: float) -> Dict[str, float]:
    if not schedule:
        return {"duration_days": 0.0}
    proj_finish_hours = max(v["finish"] for v in schedule.values())
    return {"duration_days": proj_finish_hours / max(1.0, hours_per_day)}

def analyze_bottlenecks(tasks: List[Dict[str, Any]], base_info: Dict[str, Dict[str, float]], schedule: Dict[str, Dict[str, float]]):
    """
    Return two dicts:
      - delay_by_category: total start delay vs CPM ES grouped by crew_category
      - idle_by_code: total idle time for each exact crew code (sum of gaps between consecutive tasks)
    """
    from collections import defaultdict
    delay_by_category = defaultdict(float)
    for tid, s in schedule.items():
        cat = s.get("crew_category") or "UNSPEC"
        delay_by_category[cat] += s.get("delay_vs_cpm_start", 0.0)

    # Idle by exact code
    per_code = defaultdict(list)
    for tid, s in schedule.items():
        code = s.get("crew_code") or "UNSPEC"
        per_code[code].append((s["start"], s["finish"]))
    idle_by_code = {}
    for code, intervals in per_code.items():
        intervals.sort()
        idle = 0.0
        for (a_start, a_finish), (b_start, b_finish) in zip(intervals, intervals[1:]):
            if b_start > a_finish:
                idle += (b_start - a_finish)
        idle_by_code[code] = idle
    return dict(delay_by_category), idle_by_code

def suggest_capacities_to_hit_target(tasks: List[Dict[str, Any]], base_info, hours_per_day: float,
                                     pool_by_category: bool, initial_caps: Dict[str, int],
                                     target_days: float, max_steps: int = 30):
    """
    Greedy hill-climb: repeatedly add 1 capacity to the category that yields the biggest duration reduction.
    Returns (suggested_caps, est_duration_days, steps_taken).
    """
    caps = {k: max(1, int(v)) for k, v in initial_caps.items()}
    from copy import deepcopy

    def duration_with(caps_in):
        sched = level_resources(tasks, base_info, pool_by_category=True, capacity_by_category=caps_in if caps_in else {})
        return compute_project_metrics(sched, hours_per_day)["duration_days"]

    if not pool_by_category:
        sched = level_resources(tasks, base_info, pool_by_category=False, capacity_by_category={})
        dur = compute_project_metrics(sched, hours_per_day)["duration_days"]
        return caps, dur, 0

    # Initialize missing categories to 1
    cats = set([t.get("crew_category") for t in tasks if t.get("crew_category")])
    for c in cats:
        caps.setdefault(c, 1)

    curr_dur = duration_with(caps)
    steps = 0
    while curr_dur > target_days and steps < max_steps:
        best = None
        for c in caps:
            trial = deepcopy(caps)
            trial[c] = trial[c] + 1
            d = duration_with(trial)
            improvement = curr_dur - d
            if best is None or improvement > best[0]:
                best = (improvement, c, d, trial)
        if not best or best[0] <= 1e-6:
            break
        _, chosen_c, new_dur, trial_caps = best
        caps = trial_caps
        curr_dur = new_dur
        steps += 1
    return caps, curr_dur, steps
