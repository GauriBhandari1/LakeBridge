import pandas as pd
import os
import yaml

# ========== CONFIG ==============
CONTROL_TABLE = r"D:\Lakebridge_POC_Scheduling_Test\analyzer_output\control_table_dependencies_20251101_133449.xlsx"
OUTPUT_DIR = r"D:\Lakebridge_POC_Scheduling_Test\generated_workflows"
BASE_NOTEBOOK_PATH = "/Workspace/Users/patelsatya200@gmail.com/LAKEBRIDGE_POC"
PARENT_JOB_NAME = "scheduling_parent"
# ===============================

os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pd.read_excel(CONTROL_TABLE)
df.columns = df.columns.str.lower()

required_cols = {"process_id", "process_name", "depends_on"}
if not required_cols.issubset(df.columns):
    raise Exception(f"Excel must contain columns: {required_cols}")

df["depends_on"] = df["depends_on"].fillna("").astype(str)

# ---- Generate child workflow YAMLs ----
all_jobs = {}

for _, row in df.iterrows():
    pid = row["process_id"].strip()
    pname = row["process_name"].strip()
    deps = [d.strip() for d in row["depends_on"].split(",") if d.strip()]

    notebook_path = f"{BASE_NOTEBOOK_PATH}/{pname}"

    job_yaml = {
        "resources": {
            "jobs": {
                pid: {
                    "name": pid,
                    "tasks": [
                        {
                            "task_key": pid,
                            "notebook_task": {
                                "notebook_path": notebook_path,
                                "source": "WORKSPACE"
                            },
                            # ✅ Serverless compute
                            "compute": {
                                "serverless": True  # key setting
                            }
                        }
                    ],
                    "queue": {"enabled": True},
                    "performance_target": "PERFORMANCE_OPTIMIZED"
                }
            }
        }
    }

    file_path = os.path.join(OUTPUT_DIR, f"{pid}.yml")
    with open(file_path, "w", encoding='utf-8') as f:
        yaml.dump(job_yaml, f, default_flow_style=False)

    print(f"✅ created job file: {file_path}")
    all_jobs[pid] = deps

# ---- Build Parent Workflow ----
parent_tasks = []

for pid, deps in all_jobs.items():
    task = {
        "task_key": pid,
        "run_job_task": {
            "job_id": f"<<PUT_JOB_ID_FOR_{pid}>>"
        }
    }
    if deps:
        task["depends_on"] = [{"task_key": d} for d in deps if d in all_jobs]

    parent_tasks.append(task)

parent_yaml = {
    "resources": {
        "jobs": {
            PARENT_JOB_NAME: {
                "name": PARENT_JOB_NAME,
                "tasks": parent_tasks,
                "queue": {"enabled": True}
            }
        }
    }
}

parent_file = os.path.join(OUTPUT_DIR, "parent_workflow.yml")
with open(parent_file, "w", encoding='utf-8') as f:
    yaml.dump(parent_yaml, f, default_flow_style=False)

print(f"\n🎯 Parent workflow generated ➜ {parent_file}")
print("⚠️ Replace <<PUT_JOB_ID_FOR_xxx>> after uploading jobs")
