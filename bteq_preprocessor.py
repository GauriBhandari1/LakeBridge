from pathlib import Path
import shutil

def preprocess_bteq_files(bteq_folder_str=None, teradata_input_folder_str=None):
    bteq_folder = Path(bteq_folder_str or r"C:\Users\ws_htu1651\Downloads\BTEQ_scripts")
    teradata_input_folder = Path(teradata_input_folder_str or "lakebridge/input/teradata")

    if not bteq_folder.exists():
        print(f"BTEQ folder '{bteq_folder}' does not exist. Skipping preprocessing.")
        return

    # Ensure target folder exists
    teradata_input_folder.mkdir(parents=True, exist_ok=True)

    # Recursively scan all files
    for file_path in bteq_folder.rglob("*"):
        if file_path.is_file():
            if file_path.suffix.lower() in [".btq", ".bteq"]:
                # Convert .btq/.bteq to .sql
                target_file = teradata_input_folder / (file_path.stem + ".sql")
                shutil.copy2(file_path, target_file)
                print(f"Converted: {file_path} -> {target_file}")
            elif file_path.suffix.lower() == ".sql":
                # Directly copy .sql files
                target_file = teradata_input_folder / file_path.name
                shutil.copy2(file_path, target_file)
                print(f"Copied: {file_path} -> {target_file}")

    print("BTEQ preprocessing complete.")
