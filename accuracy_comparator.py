import os
import csv

# Paths to your folders
source_folder = r"D:\Lakebridge_POC\procedures_split"
target_folder = r"D:\Lakebridge_POC\Output_Scripts_1500_scripts\Converted_Code"

# Prepare list to store results
results = []

# List all source files
source_files = [f for f in os.listdir(source_folder) if f.endswith('.sql')]

for file_name in source_files:
    source_path = os.path.join(source_folder, file_name)
    target_path = os.path.join(target_folder, file_name)

    # Get file sizes in bytes
    source_size = os.path.getsize(source_path)
    target_size = os.path.getsize(target_path) if os.path.exists(target_path) else None

    # Default status
    status = "OK"

    # Check if target file exists
    if target_size is None:
        status = "Missing in target"
        deviation_type = "N/A"
        deviation_pct = None
    else:
        # Calculate deviation percentage
        deviation_pct = 0 if source_size == 0 else abs(target_size - source_size) / source_size * 100

        if deviation_pct > 10:  # deviation threshold 10%
            status = "Review"
            deviation_type = "Less in target" if target_size < source_size else "More in target"
        else:
            deviation_type = "OK"

    results.append({
        "File Name": file_name,
        "Source Size (KB)": round(source_size/1024, 2),
        "Target Size (KB)": round(target_size/1024, 2) if target_size else None,
        "Deviation (%)": round(deviation_pct, 2) if deviation_pct is not None else None,
        "Status": status,
        "Deviation Type": deviation_type
    })

# Output CSV path
output_file = r"D:\Lakebridge_POC\lakebridge_comparison_report.csv"

# Write results to CSV
with open(output_file, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

print(f"Report generated: {output_file}")
