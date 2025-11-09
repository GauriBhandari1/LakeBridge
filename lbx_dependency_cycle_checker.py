import pandas as pd

file = r"D:\Lakebridge_POC_Scheduling_Test\analyzer_output\control_table_dependencies_20251101_133449.xlsx"

# Load data
df = pd.read_excel(file)
df.columns = df.columns.str.lower()
df["depends_on"] = df["depends_on"].fillna("").astype(str)

# Build dependency graph
graph = {
    row["process_id"]: [d.strip() for d in row["depends_on"].split(",") if d.strip()]
    for _, row in df.iterrows()
}

visited = set()
stack = set()
all_cycles = []

def detect_cycle(node, path):
    if node in path:
        cycle_start = path.index(node)
        cycle = path[cycle_start:] + [node]
        all_cycles.append(" -> ".join(cycle))
        return
    if node in visited:
        return
    
    visited.add(node)
    path.append(node)
    for nbr in graph.get(node, []):
        detect_cycle(nbr, path.copy())

for node in graph:
    detect_cycle(node, [])

if all_cycles:
    print("\n❌ CYCLES FOUND IN DEPENDENCY GRAPH:")
    unique_cycles = sorted(set(all_cycles))
    for i, cycle in enumerate(unique_cycles, 1):
        print(f"{i}. {cycle}")
else:
    print("\n✅ No cycles found. Dependency graph is valid.")
