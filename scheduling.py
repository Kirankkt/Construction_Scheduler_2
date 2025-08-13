
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, deque

def topological_order(tasks: List[Dict[str, Any]]) -> List[str]:
    indeg = defaultdict(int)
    deps = {t["id"]: set(t.get("dependencies", [])) for t in tasks}
    for t in tasks:
        indeg[t["id"]] = len(deps[t["id"]])
    q = deque([tid for tid, d in indeg.items() if d == 0])
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v, dv in deps.items():
            if u in dv:
                dv.remove(u)
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
    remaining = [tid for tid, d in indeg.items() if d > 0 and tid not in order]
    return order + remaining

def compute_cpm_baseline(tasks: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    info = {t["id"]: {"duration": float(t["duration_hours"] or 0.0)} for t in tasks}
    deps = {t["id"]: set(t.get("dependencies", [])) for t in tasks}
    order = topological_order(tasks)

    # forward
    for tid in order:
        es = 0.0
        for d in deps[tid]:
            es = max(es, info[d]["ef"])
        ef = es + info[tid]["duration"]
        info[tid]["es"] = es
        info[tid]["ef"] = ef

    proj_finish = max((info[tid]["ef"] for tid in info), default=0.0)

    # successors
    succs = {t["id"]: set() for t in tasks}
    for t in tasks:
        for d in t.get("dependencies", []):
            succs[d].add(t["id"])

    # backward
    for tid in reversed(order):
        lf = min((info[s]["ls"] for s in succs[tid]), default=proj_finish)
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
    tasks_sorted = sorted(tasks, key=lambda t: (base_info[t["id"]]["es"], t["planned_day"], t["name"]))
    schedule = {}
    if pool_by_category:
        scheduled = []
    else:
        from collections import defaultdict
        busy_until = defaultdict(float)  # crew_code -> time

    for t in tasks_sorted:
        tid = t["id"]
        dur = float(t["duration_hours"] or 0.0)
        est = max([base_info[d]["ef"] for d in t.get("dependencies", [])] + [base_info[tid]["es"]]) if t.get("dependencies") else base_info[tid]["es"]
        if not t.get("crew_category") and not t.get("crew_code"):
            start = est
        else:
            if pool_by_category:
                cat = t.get("crew_category") or "UNSPEC"
                cap = max(1, int(capacity_by_category.get(cat, 1)))
                start = est
                def active_at(tp): return sum(1 for rec in scheduled if rec["cat"] == cat and rec["start"] < tp < rec["finish"])
                while dur > 0 and active_at(start) >= cap:
                    finishes = [rec["finish"] for rec in scheduled if rec["cat"] == cat and rec["finish"] > start]
                    start = min(finishes) if finishes else start
            else:
                code = t.get("crew_code") or "UNSPEC"
                ready = busy_until[code]
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
            busy_until[code] = finish
    return schedule

def compute_project_metrics(schedule: Dict[str, Dict[str, float]], hours_per_day: float) -> Dict[str, float]:
    if not schedule:
        return {"duration_days": 0.0}
    proj_finish_hours = max(v["finish"] for v in schedule.values())
    return {"duration_days": proj_finish_hours / max(1.0, hours_per_day)}

def analyze_bottlenecks(tasks, base_info, schedule):
    from collections import defaultdict
    delay_by_category = defaultdict(float)
    for tid, s in schedule.items():
        cat = s.get("crew_category") or "UNSPEC"
        delay_by_category[cat] += s.get("delay_vs_cpm_start", 0.0)

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

def suggest_capacities_to_hit_target(tasks, base_info, hours_per_day, pool_by_category, initial_caps, target_days, max_steps=30):
    from copy import deepcopy
    caps = {k: max(1, int(v)) for k, v in initial_caps.items()}
    def duration_with(caps_in):
        sched = level_resources(tasks, base_info, pool_by_category=True, capacity_by_category=caps_in if caps_in else {})
        return compute_project_metrics(sched, hours_per_day)["duration_days"]
    if not pool_by_category:
        sched = level_resources(tasks, base_info, pool_by_category=False, capacity_by_category={})
        dur = compute_project_metrics(sched, hours_per_day)["duration_days"]
        return caps, dur, 0
    cats = set([t.get("crew_category") for t in tasks if t.get("crew_category")])
    for c in cats:
        caps.setdefault(c, 1)
    curr_dur = duration_with(caps)
    steps = 0
    while curr_dur > target_days and steps < max_steps:
        best = None
        for c in caps:
            trial = deepcopy(caps); trial[c] = trial[c] + 1
            d = duration_with(trial)
            improvement = curr_dur - d
            if best is None or improvement > best[0]:
                best = (improvement, c, d, trial)
        if not best or best[0] <= 1e-6:
            break
        _, chosen_c, new_dur, trial_caps = best
        caps = trial_caps; curr_dur = new_dur; steps += 1
    return caps, curr_dur, steps
