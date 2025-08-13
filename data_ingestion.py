import os
import re
import json
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from rapidfuzz import fuzz, process

# -------- CSV helpers --------

DAY_COL_PATTERN = re.compile(r"^Day\s*(\d+)$", re.IGNORECASE)

# Major section anchors (top-level areas)
MAJOR_SECTION_ANCHORS = [
    r"^Outside$",
    r"^Ground\s*Floor$",
    r"^(?:1st\s*Floor|First\s*Floor)$",
    r"^Roof$",
]

# Discipline anchors (global tags)
DISCIPLINE_ANCHORS = [
    r"^Waste\s*Removal$",
    r"^Termite\s*Treatment$",
    r"^Building\s*Exterior$",
    r"^Staffing\s*Needed$",
    r"^Demolition$",
    r"^Civil$",
    r"^Electrical$",
    r"^Plumbing$",
    r"^Tiling$",
    r"^Painting$",
    r"^Carpentry$",
]

def _matches_any(label: Optional[str], patterns) -> bool:
    if not label:
        return False
    return any(re.match(p, label.strip(), re.IGNORECASE) for p in patterns)

def _safe_series_get(row: pd.Series, key: Optional[str]):
    if key is None:
        return None
    try:
        return row.get(key, None)
    except Exception:
        return None

def _detect_day_triplets(columns: List[str]) -> List[Tuple[str, Optional[str], Optional[str], int]]:
    """
    Return (day_col, time_col, labour_col, day_index) for each Day N.
    Tolerates "Unnamed" columns; falls back to the next two physical columns after Day N.
    """
    days_idx = []
    for i, c in enumerate(columns):
        m = DAY_COL_PATTERN.match(str(c).strip())
        if m:
            days_idx.append((i, c, int(m.group(1))))
    days_idx.sort(key=lambda x: x[2])

    triplets: List[Tuple[str, Optional[str], Optional[str], int]] = []
    for ordinal, (i, day_col, dnum) in enumerate(days_idx):
        suffix = "" if ordinal == 0 else f".{ordinal}"
        canon_time = f"Time (hours){suffix}"
        canon_lab  = f"Labor (workers){suffix}"

        time_col = canon_time if canon_time in columns else None
        labour_col = canon_lab if canon_lab in columns else None

        if time_col is None or labour_col is None:
            nxt1 = columns[i+1] if i + 1 < len(columns) else None
            nxt2 = columns[i+2] if i + 2 < len(columns) else None
            if time_col is None and nxt1 and not DAY_COL_PATTERN.match(str(nxt1)):
                time_col = nxt1
            if labour_col is None and nxt2 and not DAY_COL_PATTERN.match(str(nxt2)):
                labour_col = nxt2

        triplets.append((day_col, time_col, labour_col, dnum))
    return triplets

def _is_section_header(row: pd.Series, triplets: List[Tuple[str, Optional[str], Optional[str], int]]) -> bool:
    """A header row has no entries in any Day/Time/Labour columns."""
    for (day_col, time_col, labour_col, _) in triplets:
        if pd.notna(_safe_series_get(row, day_col)) \
           or pd.notna(_safe_series_get(row, time_col)) \
           or pd.notna(_safe_series_get(row, labour_col)):
            return False
    return True

def _clean_str(x: Any) -> Optional[str]:
    if pd.isna(x): return None
    s = str(x).strip()
    return s if s else None

def parse_csv_to_tasks(csv_path: str,
                       working_hours_per_day: float = 8.0,
                       auto_chain_within_subsection: bool = True):
    """
    Parse wide CSV to flat tasks. No imputation (missing durations remain None).
    Task schema: id, section, subsection, discipline, name, planned_day, duration_hours,
                 crew_code, crew_category, dependencies[]
    """
    df = pd.read_csv(csv_path)
    columns = list(df.columns)
    row_label_col = columns[0]
    triplets = _detect_day_triplets(columns)
    warnings = []
    if not triplets:
        warnings.append("No 'Day N' columns found. Please verify the CSV structure.")
        return [], warnings

    tasks: List[Dict[str, Any]] = []
    current_section: Optional[str] = None
    current_discipline: Optional[str] = None
    task_counter = 0

    for _, row in df.iterrows():
        label = _clean_str(_safe_series_get(row, row_label_col))

        # Explicit anchors have priority
        if _matches_any(label, MAJOR_SECTION_ANCHORS):
            current_section = label
            current_discipline = None
            continue
        if _matches_any(label, DISCIPLINE_ANCHORS):
            current_discipline = label
            continue

        # Conservative: if row looks like an empty header, we don't auto-promote it to section
        if _is_section_header(row, triplets):
            # Could set current_section = label here, but that caused noise; skip
            continue

        # Otherwise it's a Subsection line under the current major Section
        subsection = label
        for (day_col, time_col, labour_col, dnum) in triplets:
            name = _clean_str(_safe_series_get(row, day_col))
            if not name:
                continue
            name = re.sub(r"\s+,", ",", name).strip().rstrip(",")

            dur_val = _safe_series_get(row, time_col)
            duration_hours = float(dur_val) if pd.notna(dur_val) else None

            labour_val = _clean_str(_safe_series_get(row, labour_col))
            crew_code = None
            crew_cat = None
            if labour_val:
                crew_code = str(labour_val).strip()
                m = re.match(r"^\s*(\d+)(?:\.\d+)?\s*$", crew_code)
                if m:
                    crew_cat = m.group(1)

            task_id = f"T{task_counter:04d}"
            task_counter += 1
            tasks.append({
                "id": task_id,
                "section": current_section,
                "subsection": subsection,
                "discipline": current_discipline,   # NEW: tag like Demolition/Electrical/etc.
                "name": name,
                "planned_day": int(dnum),
                "duration_hours": duration_hours,   # may be None (no imputation)
                "crew_code": crew_code,
                "crew_category": crew_cat,
                "dependencies": []
            })

    # Auto-chain within (section, subsection) by ascending planned_day
    if auto_chain_within_subsection:
        from collections import defaultdict
        by_group = defaultdict(list)
        for t in tasks:
            key = (t["section"], t["subsection"])
            by_group[key].append(t)
        for key, items in by_group.items():
            items.sort(key=lambda x: (x["planned_day"], x["name"]))
            for prev, cur in zip(items, items[1:]):
                cur["dependencies"].append(prev["id"])

    return tasks, warnings

# -------- PDF cache (pdfplumber) --------

def _pdf_cache_file(cache_dir: str = "data") -> str:
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "drawing_notes_cache.json")

def _quick_sig(path: str):
    st = os.stat(path)
    return {"size": int(st.st_size), "mtime": int(st.st_mtime)}

def _parse_pdf_notes_with_pdfplumber(pdf_path: str):
    try:
        import pdfplumber
    except Exception:
        return []
    notes, seen = [], set()
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw in text.splitlines():
                line = raw.strip()
                if line.lower().startswith("note -"):
                    content = line[6:].strip()
                    if content and content not in seen:
                        notes.append(content)
                        seen.add(content)
    return notes

def load_drawing_notes_from_cache(cache_dir: str = "data"):
    cache_path = _pdf_cache_file(cache_dir)
    if not os.path.exists(cache_path):
        return []
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except Exception:
        return []
    out = []
    for rec in cache.values():
        out.extend(rec.get("notes", []))
    return out

def rebuild_drawing_notes_cache(pdf_paths: List[str], cache_dir: str = "data"):
    cache_path = _pdf_cache_file(cache_dir)
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except Exception:
        cache = {}
    changed = False
    for p in pdf_paths:
        key = os.path.basename(p)
        sig = _quick_sig(p)
        rec = cache.get(key)
        if rec and rec.get("sig") == sig:
            continue
        notes = _parse_pdf_notes_with_pdfplumber(p)
        cache[key] = {"sig": sig, "notes": notes}
        changed = True
    if changed:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    return load_drawing_notes_from_cache(cache_dir)

# -------- Fuzzy note↔task matching --------

def match_notes_to_tasks(notes: List[str], tasks: List[Dict[str, Any]], limit: int = 3):
    names = {t["id"]: t["name"] for t in tasks}
    name_list = list(names.values())
    id_by_name = {names[k]: k for k in names}
    results = []
    for note in notes:
        matches = process.extract(note, name_list, scorer=fuzz.token_set_ratio, limit=limit)
        results.append({
            "note": note,
            "matches": [(id_by_name[name], name, int(score)) for name, score, _ in matches]
        })
    return results
