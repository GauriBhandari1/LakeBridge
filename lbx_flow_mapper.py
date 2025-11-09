"""
build_control_table_v2.py

Usage:
    - Ensure pandas and openpyxl are installed:
        pip install pandas openpyxl

    - Update `input_file` if needed and run:
        python build_control_table_v2.py
"""

import pandas as pd
import re
import os
from pathlib import Path
from datetime import datetime

# -------- CONFIG --------
input_file = Path(r"D:\Lakebridge_POC_Scheduling_Test\analyzer_output\lakebridge_analysis_20251009_113631.xlsx")
sheet_name = "RAW_PROGRAM_OBJECT_XREF"
out_folder = input_file.parent
# ✅ Generate timestamp
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

# ✅ Output files WITH timestamp
output_control = out_folder / f"control_table_dependencies_{ts}.xlsx"
output_map = out_folder / f"table_writer_map_{ts}.xlsx"

# -------- HELPERS --------
def normalize_identifier(x: str) -> str:
    """Remove brackets, extra spaces and lowercase"""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = s.replace("[", "").replace("]", "")
    s = re.sub(r"\s+", "", s)
    return s.lower()

def short_table_name(full_table: str) -> str:
    """
    Convert 'edwStg.stg2C_CAM' -> 'stg2c_cam' OR 'schema.table' -> 'table'
    Keeps only the last identifier after dot.
    """
    if not full_table:
        return ""
    t = normalize_identifier(full_table)
    if "." in t:
        return t.split(".")[-1]
    return t

def program_basename(prog_path: str) -> str:
    """
    Extract filename without extension from program path
    e.g. D:/.../PopulateDimAccount_01_stg2GAM.sql -> populatedimaccount_01_stg2gam
    """
    if not prog_path:
        return ""
    p = str(prog_path).strip().replace("\\", "/").split("/")[-1]
    p = re.sub(r"\.sql$", "", p, flags=re.I)
    return normalize_identifier(p)

def detect_writer_by_operation(op: str) -> bool:
    """Return True if operation indicates a writer"""
    if not isinstance(op, str):
        return False
    op = op.strip().lower()
    writer_keywords = ("create", "insert", "truncate", "write", "update", "drop", "select into")
    return any(k in op for k in writer_keywords)

# -------- Step 1: Read data --------
df_raw = pd.read_excel(input_file, sheet_name=sheet_name, dtype=str)
# standardize column names
df_raw.columns = [c.strip() for c in df_raw.columns]

# expected columns: Program, Object, maybe Operation
# make lowercase keys for internal use
col_map = {}
for c in df_raw.columns:
    lc = c.strip().lower()
    if "program" in lc:
        col_map["program"] = c
    elif "object" in lc:
        col_map["object"] = c
    elif "operation" in lc:
        col_map["operation"] = c

if "program" not in col_map or "object" not in col_map:
    raise SystemExit("Input sheet must contain 'Program' and 'Object' columns (case-insensitive).")

df = pd.DataFrame()
df["program_raw"] = df_raw[col_map["program"]].fillna("").astype(str)
df["object_raw"] = df_raw[col_map["object"]].fillna("").astype(str)
if "operation" in col_map:
    df["operation_raw"] = df_raw[col_map["operation"]].fillna("").astype(str)
else:
    df["operation_raw"] = ""

# -------- Step 2: Normalize names & extract fields --------
df["program"] = df["program_raw"].apply(normalize_identifier)
df["program_basename"] = df["program_raw"].apply(program_basename)
df["object"] = df["object_raw"].apply(normalize_identifier)
df["table_short"] = df["object_raw"].apply(short_table_name)
df["operation"] = df["operation_raw"].apply(lambda x: str(x).strip().lower())

# -------- Step 3: Determine candidate writers using Operation column --------
# Map: table -> set(programs that write it)
table_writers = {}

for _, row in df.iterrows():
    tbl = row["object"]
    prog = row["program"]
    op = row["operation"]
    if detect_writer_by_operation(op):
        table_writers.setdefault(tbl, set()).add(prog)

# -------- Step 4: Use filename heuristic to detect writers where operation not present --------
# Heuristic: if program basename contains the short table name -> it's likely a writer
# Example: program basename 'populatedimaccount_01_stg2gam' contains 'stg2gam'
programs = df[["program", "program_basename"]].drop_duplicates()
for _, p_row in programs.iterrows():
    prog = p_row["program"]
    base = p_row["program_basename"]
    if not base:
        continue
    # check all unique tables, match short name in basename
    for tbl in df["object"].unique():
        if not tbl:
            continue
        st = short_table_name(tbl)
        if not st:
            continue
        # match whole short token in basename (use delimiters or substring)
        # split basename into tokens by non-alnum to avoid accidental matches
        tokens = re.split(r'[^0-9a-zA-Z_]+', base)
        tokens = [t for t in tokens if t]
        if st in tokens or st in base:
            # add as candidate writer only if not already marked by operation (operation is stronger)
            if tbl not in table_writers:
                table_writers.setdefault(tbl, set()).add(prog)

# -------- Step 5: Build canonical list of processes (SPs) from programs that appear in the data --------
# Each distinct program path -> a process (use program basename as process_name)
unique_programs = df[["program", "program_basename"]].drop_duplicates().reset_index(drop=True)
unique_programs["process_name"] = unique_programs["program_basename"].replace("", "unknown_program")
# create process_id mapping
process_map = {}
for i, (_, row) in enumerate(unique_programs.iterrows(), start=1):
    process_map[row["program"]] = f"proc_{i:04d}"
# inverse lookup: process_id -> process_name
procid_to_name = {pid: unique_programs.loc[idx, "process_name"] for idx, pid in enumerate(process_map.values())}

# -------- Step 6: Create orphan processes for tables that have no writer detected --------
all_tables = sorted(set(df["object"].unique()) - {""})
orphan_entries = []  # list of dicts: table, process_name, process_id

# Helper to get writer process ids for a table (may be multiple)
def get_writer_procs_for_table(tbl):
    """Return list of process_ids that write this table (possibly empty)"""
    writers = table_writers.get(tbl, set())
    pids = []
    for w in writers:
        pid = process_map.get(w)
        if pid:
            pids.append(pid)
    return sorted(list(set(pids)))

# Find tables with no detected writer
for tbl in all_tables:
    writer_pids = get_writer_procs_for_table(tbl)
    if not writer_pids:
        # create orphan process_name & id
        orphan_proc_name = f"orphaned_{tbl.replace('.', '_')}"
        # ensure unique process_id
        next_idx = len(process_map) + 1
        orphan_pid = f"proc_{next_idx:04d}"
        # add to maps
        process_map[orphan_proc_name] = orphan_pid  # note: process_map keys are program identifiers, but we'll store orphan as key too
        procid_to_name[orphan_pid] = orphan_proc_name
        orphan_entries.append({"table": tbl, "process_name": orphan_proc_name, "process_id": orphan_pid})

# After orphan addition, we need a complete mapping of program -> process_id AND orphan process names -> ids
# Build a reverse map: process_name (string) -> process_id
process_name_to_id = {}
# from actual programs
for prog, pid in process_map.items():
    # If prog is program path (contains slash or backslash), get its basename as process_name
    if "/" in prog or "\\" in prog or prog.endswith(".sql") or len(prog) > 0:
        pname = program_basename(prog)
        if not pname:
            pname = prog
        process_name_to_id[pname] = pid
    else:
        # prog might be orphan name we added earlier
        process_name_to_id[prog] = pid

# include orphan entries explicitly
for oe in orphan_entries:
    process_name_to_id[oe["process_name"]] = oe["process_id"]

# -------- Step 7: Build dependency for each process (program) ----------------
# Idea:
# For each program P:
#   - get set of objects it reads (if Operation exists and is READ, or if Operation missing assume READ)
#   - for each object, find writer process_id(s) (from table_writers) or orphan process id
#   - depends_on = unique list of writer process_ids (excluding self if present)
dependency_rows = []

# Determine read relationships: if operation contains 'read' -> treat as read; if no operation data -> assume read
def is_read_operation(op: str) -> bool:
    if not op:
        return True
    return "read" in op or "select" in op

program_list = unique_programs["program"].tolist()

for prog in program_list:
    prog_basename = program_basename(prog)
    pid = process_map.get(prog)
    if not pid:
        continue
    # tables used by this prog
    rows_for_prog = df[df["program"] == prog]
    # if operation column present, consider only rows with read-like ops; if operation empty overall we treat all objects as read
    read_tables = set()
    for _, r in rows_for_prog.iterrows():
        op = r["operation"]
        tbl = r["object"]
        if is_read_operation(op):
            read_tables.add(tbl)
    # Now find dependencies (writer pids or orphan pids)
    depends = []
    for tbl in read_tables:
        writer_pids = get_writer_procs_for_table(tbl)
        if writer_pids:
            depends.extend(writer_pids)
        else:
            # find orphan pid created earlier
            orphan_name = f"orphaned_{tbl.replace('.', '_')}"
            orphan_pid = process_name_to_id.get(orphan_name)
            if orphan_pid:
                depends.append(orphan_pid)
            else:
                # extra fallback: try to find a program whose basename contains the short table name
                st = short_table_name(tbl)
                found = []
                for cand_prog, cand_pid in process_map.items():
                    if st and st in program_basename(cand_prog):
                        found.append(cand_pid)
                if found:
                    depends.extend(found)
                else:
                    # as last resort, create an orphan on the fly
                    next_idx = len(process_name_to_id) + 1
                    orphan_pid = f"proc_{next_idx:04d}"
                    orphan_name = f"orphaned_{tbl.replace('.', '_')}"
                    process_name_to_id[orphan_name] = orphan_pid
                    procid_to_name[orphan_pid] = orphan_name
                    depends.append(orphan_pid)

    # remove self-dependency if present
    depends = sorted(set([d for d in depends if d != pid]))
    dependency_rows.append({
        "process_id": pid,
        "process_name": prog_basename if prog_basename else prog,
        "depends_on": ",".join(depends)
    })

# -------- Step 8: Add orphan-only rows to dependency_rows (if not already present) --------
for oe in orphan_entries:
    if oe["process_id"] not in [r["process_id"] for r in dependency_rows]:
        dependency_rows.append({
            "process_id": oe["process_id"],
            "process_name": oe["process_name"],
            "depends_on": ""
        })

# There might be programs (writers) that were never seen as program in unique_programs (rare). Add them:
# Also ensure uniqueness by process_id
seen_pids = {r["process_id"] for r in dependency_rows}
for pname, pid in process_name_to_id.items():
    if pid not in seen_pids:
        dependency_rows.append({
            "process_id": pid,
            "process_name": pname,
            "depends_on": ""
        })
        seen_pids.add(pid)

# Sort rows by process_id for readability
dependency_rows = sorted(dependency_rows, key=lambda r: r["process_id"])

final_df = pd.DataFrame(dependency_rows, columns=["process_id", "process_name", "depends_on"])

# -------- Step 9: Create table -> writer process map for verification --------
table_map_rows = []
for tbl in all_tables:
    writer_pids = get_writer_procs_for_table(tbl)
    if writer_pids:
        writer_names = [procid_to_name.get(pid, "") for pid in writer_pids]
    else:
        orphan_name = f"orphaned_{tbl.replace('.', '_')}"
        orphan_pid = process_name_to_id.get(orphan_name, "")
        writer_pids = [orphan_pid] if orphan_pid else []
        writer_names = [orphan_name] if orphan_name else []
    table_map_rows.append({
        "table": tbl,
        "table_short": short_table_name(tbl),
        "writer_process_ids": ",".join(writer_pids),
        "writer_process_names": ",".join(writer_names)
    })

table_map_df = pd.DataFrame(table_map_rows)

# -------- Step 10: Save outputs --------
final_df.to_excel(output_control, index=False)
table_map_df.to_excel(output_map, index=False)

print(f"✅ Control table written to: {output_control}")
print(f"✅ Table->writer map written to: {output_map}")

