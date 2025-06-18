import os
import sys
import time
import openpyxl
from datetime import datetime
import pandas as pd

# Add project path
sys.path.append("C:/Users/DF Team 03/Music/receipe_management/Batch_Management")

# Import all necessary modules
from demo_fng import extract_and_generate_sql
from demo_datex_mec import generate_datex_sql1
from demo_raw_material_mec import generate_raw_material_sql1
from demo_instruction_steps_mec import generate_instruction_steps_sql1
from demo_instruction_items_mec import generate_instr_raw_step_material_sql1


# Define folders
WATCH_FOLDER = "C:/Users/DF Team 03/Music/receipe_management/Batch_Management/automatic_batch_receipe/watch_folder"
LOG_FOLDER = "C:/Users/DF Team 03/Music/receipe_management/Batch_Management/automatic_batch_receipe/logs"
SQL_FOLDER = "C:/Users/DF Team 03/Music/receipe_management/Batch_Management/automatic_batch_receipe/sqls"

# Create folders if they don't exist
os.makedirs(LOG_FOLDER, exist_ok=True)
os.makedirs(SQL_FOLDER, exist_ok=True)

# ✅ FIXED: Now accepts item_number as second argument
def process_file(file_path, item_number):
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = os.path.splitext(os.path.basename(file_path))[0]
 
        # === Process Finished Goods ===
        fng_sql_file = os.path.join(SQL_FOLDER, f"{base_name}_finished_goods_{timestamp}.sql")
        fng_log_file = os.path.join(LOG_FOLDER, f"{base_name}_finished_goods_{timestamp}.log")
        fng_success, fng_message = extract_and_generate_sql(file_path, fng_sql_file, item_number)

        with open(fng_log_file, 'w', encoding='utf-8') as f:
            f.write(f"Processed File: {file_path}\n")
            f.write(f"SQL Output: {fng_sql_file}\n")
            f.write(f"Status: {'Success' if fng_success else 'Failed'}\n")
            f.write(f"Message:\n{fng_message}\n")

        # === Process Datex ===
        datex_sql_file = os.path.join(SQL_FOLDER, f"{base_name}_datex_{timestamp}.sql")
        datex_log_file = os.path.join(LOG_FOLDER, f"{base_name}_datex_{timestamp}.log")
        datex_success, datex_message = generate_datex_sql1(file_path, datex_sql_file, item_number)

        with open(datex_log_file, 'w', encoding='utf-8') as f:
            f.write(f"Processed File: {file_path}\n")
            f.write(f"SQL Output: {datex_sql_file}\n")
            f.write(f"Status: {'Success' if datex_success else 'Failed'}\n")
            f.write(f"Message:\n{datex_message}\n")

        # === Process Raw Material ===
        raw_sql_file = os.path.join(SQL_FOLDER, f"{base_name}_raw_material_{timestamp}.sql")
        raw_log_file = os.path.join(LOG_FOLDER, f"{base_name}_raw_material_{timestamp}.log")
        raw_success, raw_message = generate_raw_material_sql1(file_path, raw_sql_file, item_number, timestamp)

        with open(raw_log_file, 'w', encoding='utf-8') as f:
            f.write(f"Processed File: {file_path}\n")
            f.write(f"SQL Output: {raw_sql_file}\n")
            f.write(f"Status: {'Success' if raw_success else 'Failed'}\n")
            f.write(f"Message:\n{raw_message}\n")

        # === Process Instruction Steps ===
        instr_sql_file = os.path.join(SQL_FOLDER, f"{base_name}_instruction_steps_{timestamp}.sql")
        instr_log_file = os.path.join(LOG_FOLDER, f"{base_name}_instruction_steps_{timestamp}.log")
        instr_success, instr_message = generate_instruction_steps_sql1(file_path, instr_sql_file, item_number, timestamp)

        with open(instr_log_file, 'w', encoding='utf-8') as f:
            f.write(f"Processed File: {file_path}\n")
            f.write(f"SQL Output: {instr_sql_file}\n")
            f.write(f"Status: {'Success' if instr_success else 'Failed'}\n")
            f.write(f"Message:\n{instr_message}\n")

        # === Process Instruction Raw Step Materials ===
        instr_raw_sql_file = os.path.join(SQL_FOLDER, f"{base_name}_instruction_raw_steps_{timestamp}.sql")
        instr_raw_log_file = os.path.join(LOG_FOLDER, f"{base_name}_instruction_raw_steps_{timestamp}.log")
        instr_raw_success, instr_raw_message = generate_instr_raw_step_material_sql1(file_path, instr_raw_sql_file, item_number, timestamp)

        with open(instr_raw_log_file, 'w', encoding='utf-8') as f:
            f.write(f"Processed File: {file_path}\n")
            f.write(f"SQL Output: {instr_raw_sql_file}\n")
            f.write(f"Status: {'Success' if instr_raw_success else 'Failed'}\n")
            f.write(f"Message:\n{instr_raw_message}\n")

        print(f"✅ Logs created for all 5 SQL scripts for: {base_name}")
        return True  # ✅ indicate success to watcher

    except Exception as e:
        print(f"❌ Critical error during file processing: {e}")
        return False

# ✅ Watch folder loop
def watch_folder_loop():
    print(f"📂 Watching folder: {WATCH_FOLDER}")
    processed_files = set()
    item_number = 50  # Starting item number

    while True:
        try:
            files = [f for f in os.listdir(WATCH_FOLDER) if f.lower().endswith('.xlsx')]
            for file in files:
                file_path = os.path.join(WATCH_FOLDER, file)

                if file_path not in processed_files and os.path.isfile(file_path):
                    print(f"🔍 Found new file: {file}")

                    # ✅ Pass both file_path and item_number
                    success = process_file(file_path, item_number)
                    if success:
                        processed_files.add(file_path)
                        item_number += 1
               
        except Exception as e:
            print(f"⚠️ Folder watch error: {e}")

        time.sleep(5)

if __name__ == "__main__":
    watch_folder_loop()
