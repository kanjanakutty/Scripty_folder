import pandas as pd
import os
from datetime import datetime

def generate_datex_sql1(xlsx_file, output_folder, fng_item_number):
    try:
        sheet_name = "DATEX"

        # Read the raw Excel data
        df_raw = pd.read_excel(
            xlsx_file,
            sheet_name=sheet_name,
            usecols='C:H',
            header=None,
            skiprows=10
        )

        # Extract headers
        headers = [str(col).strip() for col in df_raw.iloc[0]]
        df = df_raw[1:].copy()
        df.columns = headers

        # Filter valid rows
        df = df[df['Item Number'].notna()]
        df = df[df['Vendor'].notna()]

        # Add FNG Item Number column
        df['Fngnumber'] = fng_item_number
        df['rm_item_id'] = df.index + 1
        # Mapping headers to SQL column names
        header_to_sql = {  
            'rm_item_id'  :'rm_item_id',    
            'Fngnumber': 'Fngnumber',
            'Item Number': 'Item_number',
            'Vendor': 'vendor',
            'Item': 'item',
            'Type': 'type',
            'Lbs. / Batch': 'LBS_per_batch',
            'Lbs / Gal.': 'LBS_per_Gal',          
        }

        # Check for missing required headers
        required_headers = list(header_to_sql.keys())
        missing_headers = [h for h in required_headers if h not in df.columns]
        if missing_headers:
            raise ValueError(f"Missing required headers: {missing_headers}")

        # SQL column list
        sql_headers = [header_to_sql[h] for h in required_headers]

        # Start SQL INSERT
        #sql_query = f"INSERT INTO demo_work.batch_datex ({', '.join([f'`{h}`' for h in sql_headers])}) VALUES\n"
        sql_query = f"INSERT INTO demo_work.batch_datex ({', '.join(sql_headers)}) VALUES\n"

        # Format values
        values = []
        for _, row in df.iterrows():
            formatted_row = []
            for h in required_headers:
                val = row[h]
                if pd.isna(val):
                    formatted_row.append("NULL")
                else:
                    val_str = str(val).replace("'", "''")
                    formatted_row.append(f"'{val_str}'")
            values.append(f"({', '.join(formatted_row)})")

        # Complete SQL
        sql_query += ",\n".join(values) + ";"

        # Save to file
        os.makedirs(output_folder, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_sql_path = os.path.join(output_folder, f"insert_batch_datex_{timestamp}.sql")

        with open(output_sql_path, "w", encoding="utf-8") as f:
            f.write(sql_query)

        success_message = f"✅ DATEX SQL generated successfully: {output_sql_path}"
        print(success_message)
        return True, success_message

    except Exception as e:
        error_msg = f"❌ Error generating DATEX SQL: {e}"
        print(error_msg)
        return False, error_msg
