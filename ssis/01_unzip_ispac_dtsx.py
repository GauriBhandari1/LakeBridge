from pathlib import Path
import shutil
import tempfile
import zipfile

# --- USER ONLY MODIFIES THIS ---
USER_INPUT = Path(r"C:\Users\ws_htu1651\Downloads\ssis_scripts_check")  # <-- user sets their files here

# Fixed LakeBridge input folder
LAKEBRIDGE_INPUT = Path(__file__).parent.parent / "lakebridge" / "input" / "ssis"
LAKEBRIDGE_INPUT.mkdir(parents=True, exist_ok=True)

def extract_dtsx_from_ispac(ispac_file, output_dir):
    try:
        with zipfile.ZipFile(ispac_file, 'r') as z:
            for name in z.namelist():
                if name.lower().endswith(".dtsx"):
                    base_name = Path(ispac_file).stem
                    dest_file = output_dir / f"{base_name}_{Path(name).name}"
                    counter = 1
                    while dest_file.exists():
                        dest_file = output_dir / f"{base_name}_{counter}_{Path(name).name}"
                        counter += 1
                    with z.open(name) as src, open(dest_file, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                    print(f"➤ Extracted {dest_file.name}")
    except zipfile.BadZipFile:
        print(f"❌ Invalid .ispac file: {ispac_file}")

def process_path(input_path, output_dir):
    if not input_path.exists():
        print(f"❌ Input path does not exist: {input_path}")
        return
    if input_path.is_file():
        if input_path.suffix.lower() == ".zip":
            with tempfile.TemporaryDirectory() as tmp:
                with zipfile.ZipFile(input_path, 'r') as z:
                    z.extractall(tmp)
                for f in Path(tmp).rglob("*"):
                    process_path(f, output_dir)
        elif input_path.suffix.lower() == ".ispac":
            extract_dtsx_from_ispac(input_path, output_dir)
        elif input_path.suffix.lower() == ".dtsx":
            shutil.copy(input_path, output_dir / input_path.name)
            print(f"➤ Copied {input_path.name}")
    elif input_path.is_dir():
        for f in input_path.iterdir():
            process_path(f, output_dir)

def run():
    print(f"\n🔍 Processing user input: {USER_INPUT}")
    process_path(USER_INPUT, LAKEBRIDGE_INPUT)
    print(f"\n✅ All DTSX files copied to {LAKEBRIDGE_INPUT}")
