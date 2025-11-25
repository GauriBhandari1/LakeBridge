import requests, pandas as pd, json, os, warnings
warnings.filterwarnings("ignore")

DATABRICKS_URL = "https://dbc-0668e0d5-07bc.cloud.databricks.com"
TOKEN = "dapib49b04057228e3e837bba4af70064971"

headers = {"Authorization": f"Bearer {TOKEN}"}

control_file = r"D:\Lakebridge_POC_Scheduling_Test\analyzer_output\control_table_dependencies_20251101_133449.xlsx"
df = pd.read_excel(control_file)

job_ids = {}

def create_child_job(proc_id, proc_name):
    payload = {
        "name": proc_id,
        "tasks": [
            {
                "task_key": proc_id,
                "notebook_task": {
                    "notebook_path": f"/Workspace/Users/patelsatya200@gmail.com/LAKEBRIDGE_POC/{proc_name}",
                    "source": "WORKSPACE"
                },
                "compute": {      # ✅ Serverless compute — required
                    "serverless": True
                }
            }
        ],
        "queue": {"enabled": True},
        "max_concurrent_runs": 1
    }

    r = requests.post(
        f"{DATABRICKS_URL}/api/2.1/jobs/create",
        headers=headers, json=payload, verify=False
    )

    if r.status_code == 200:
        job_id = r.json()["job_id"]
        job_ids[proc_id] = job_id
        print(f"✅ Created job {proc_id} → job_id: {job_id}")
    else:
        print(f"❌ Failed to create job {proc_id} → {r.text}")


# ✅ Step-1 Create all child jobs
print("\n=== ✅ STEP-1: Creating Child Jobs ===")
for _, row in df.iterrows():
    create_child_job(row["process_id"], row["process_name"])

# ✅ Step-2 Create parent workflow
parent_tasks = []

for _, row in df.iterrows():
    pid = row["process_id"]
    deps = str(row["depends_on"]).strip()

    dep_list = []
    if deps and deps != "nan":
        for d in deps.split(","):
            d = d.strip()
            if d in job_ids:
                dep_list.append({"task_key": d})

    parent_tasks.append({
        "task_key": pid,
        "run_job_task": {"job_id": job_ids.get(pid)},
        "depends_on": dep_list
    })

parent_payload = {
    "name": "Lakebridge_Universal_Workflow",
    "tasks": parent_tasks,
    "queue": {"enabled": True}
}

print("\n=== ✅ STEP-2: Creating Parent Workflow ===")
resp = requests.post(
    f"{DATABRICKS_URL}/api/2.1/jobs/create",
    headers=headers, json=parent_payload, verify=False
)

if resp.status_code == 200:
    print(f"✅ Parent Workflow Created: {resp.json()['job_id']}")
else:
    print(f"❌ Parent Workflow Failed → {resp.text}")
