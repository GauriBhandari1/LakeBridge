"""
build_control_table_v2.py

Usage:
    - Ensure pandas, openpyxl, and PyYAML are installed:
        pip install pandas openpyxl pyyaml

    - Run directly:
        python build_control_table_v2.py
"""

import pandas as pd
import re
import os
import yaml
from pathlib import Path
from datetime import datetime

# -------- CONFIG --------

# Load dialect and paths dynamically
config_path = Path("config.yaml")
if config_path.exists():
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    dialect = config.get("dialect", "Synapse").lower()
    base_input_path = Path(config.get("source_path", "./lakebridge/input"))
    base_output_path = Path(config.get("target_path", "./lakebridge/output"))
else:
    dialect = "synapse"
    base_input_path = Path("./lakebridge/input")
    base_output_path = Path("./lakebridge/output")

# Construct dialect-specific input/output folders
input_dir = base_input_path / dialect
output_dir = base_output_path / dialect / "analyzer_output"

# Automatically pick latest Excel file in analyzer_output
excel_files = sorted(output_dir.glob("*.xlsx"), key=os.path.getmtime, reverse=True)
if not excel_files:
    raise SystemExit(f"❌ No analyzer Excel files found in {output_dir}")

input_file = excel_files[0]
sheet_name = "RAW_PROGRAM_OBJECT_XREF"
out_folder = input_file.parent

# ✅ Generate timestamp
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

# ✅ Output files WITH timestamp
output_control = out_folder / f"control_table_dependencies_{ts}.xlsx"
output_map = out_folder / f"table_writer_map_{ts}.xlsx"

print(f"📘 Using dialect: {dialect}")
print(f"📂 Input file: {input_file}")
print(f"📁 Output folder: {out_folder}")

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
df_raw.columns = [c.strip() for c in df_raw.columns]

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

# -------- Step 3–10 (unchanged) --------
# ✅ The rest of your original code is untouched
# (same logic for dependency mapping, orphans, writer detection, etc.)

# -------- Step 3: Determine candidate writers using Operation column --------
table_writers = {}

for _, row in df.iterrows():
    tbl = row["object"]
    prog = row["program"]
    op = row["operation"]
    if detect_writer_by_operation(op):
        table_writers.setdefault(tbl, set()).add(prog)

# -------- Step 4: Filename heuristic --------
programs = df[["program", "program_basename"]].drop_duplicates()
for _, p_row in programs.iterrows():
    prog = p_row["program"]
    base = p_row["program_basename"]
    if not base:
        continue
    for tbl in df["object"].unique():
        if not tbl:
            continue
        st = short_table_name(tbl)
        if not st:
            continue
        tokens = re.split(r'[^0-9a-zA-Z_]+', base)
        tokens = [t for t in tokens if t]
        if st in tokens or st in base:
            if tbl not in table_writers:
                table_writers.setdefault(tbl, set()).add(prog)

unique_programs = df[["program", "program_basename"]].drop_duplicates().reset_index(drop=True)
unique_programs["process_name"] = unique_programs["program_basename"].replace("", "unknown_program")
process_map = {}
for i, (_, row) in enumerate(unique_programs.iterrows(), start=1):
    process_map[row["program"]] = f"proc_{i:04d}"
procid_to_name = {pid: unique_programs.loc[idx, "process_name"] for idx, pid in enumerate(process_map.values())}

all_tables = sorted(set(df["object"].unique()) - {""})
orphan_entries = []

def get_writer_procs_for_table(tbl):
    writers = table_writers.get(tbl, set())
    pids = []
    for w in writers:
        pid = process_map.get(w)
        if pid:
            pids.append(pid)
    return sorted(list(set(pids)))

for tbl in all_tables:
    writer_pids = get_writer_procs_for_table(tbl)
    if not writer_pids:
        orphan_proc_name = f"orphaned_{tbl.replace('.', '_')}"
        next_idx = len(process_map) + 1
        orphan_pid = f"proc_{next_idx:04d}"
        process_map[orphan_proc_name] = orphan_pid
        procid_to_name[orphan_pid] = orphan_proc_name
        orphan_entries.append({"table": tbl, "process_name": orphan_proc_name, "process_id": orphan_pid})

process_name_to_id = {}
for prog, pid in process_map.items():
    if "/" in prog or "\\" in prog or prog.endswith(".sql") or len(prog) > 0:
        pname = program_basename(prog)
        if not pname:
            pname = prog
        process_name_to_id[pname] = pid
    else:
        process_name_to_id[prog] = pid

for oe in orphan_entries:
    process_name_to_id[oe["process_name"]] = oe["process_id"]

dependency_rows = []

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
    rows_for_prog = df[df["program"] == prog]
    read_tables = set()
    for _, r in rows_for_prog.iterrows():
        op = r["operation"]
        tbl = r["object"]
        if is_read_operation(op):
            read_tables.add(tbl)
    depends = []
    for tbl in read_tables:
        writer_pids = get_writer_procs_for_table(tbl)
        if writer_pids:
            depends.extend(writer_pids)
        else:
            orphan_name = f"orphaned_{tbl.replace('.', '_')}"
            orphan_pid = process_name_to_id.get(orphan_name)
            if orphan_pid:
                depends.append(orphan_pid)
            else:
                st = short_table_name(tbl)
                found = []
                for cand_prog, cand_pid in process_map.items():
                    if st and st in program_basename(cand_prog):
                        found.append(cand_pid)
                if found:
                    depends.extend(found)
                else:
                    next_idx = len(process_name_to_id) + 1
                    orphan_pid = f"proc_{next_idx:04d}"
                    orphan_name = f"orphaned_{tbl.replace('.', '_')}"
                    process_name_to_id[orphan_name] = orphan_pid
                    procid_to_name[orphan_pid] = orphan_name
                    depends.append(orphan_pid)
    depends = sorted(set([d for d in depends if d != pid]))
    dependency_rows.append({
        "process_id": pid,
        "process_name": prog_basename if prog_basename else prog,
        "depends_on": ",".join(depends)
    })

for oe in orphan_entries:
    if oe["process_id"] not in [r["process_id"] for r in dependency_rows]:
        dependency_rows.append({
            "process_id": oe["process_id"],
            "process_name": oe["process_name"],
            "depends_on": ""
        })

seen_pids = {r["process_id"] for r in dependency_rows}
for pname, pid in process_name_to_id.items():
    if pid not in seen_pids:
        dependency_rows.append({
            "process_id": pid,
            "process_name": pname,
            "depends_on": ""
        })
        seen_pids.add(pid)

dependency_rows = sorted(dependency_rows, key=lambda r: r["process_id"])

final_df = pd.DataFrame(dependency_rows, columns=["process_id", "process_name", "depends_on"])

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

final_df.to_excel(output_control, index=False)
table_map_df.to_excel(output_map, index=False)

print(f"✅ Control table written to: {output_control}")
print(f"✅ Table->writer map written to: {output_map}")
