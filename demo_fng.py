import pandas as pd
import os
from datetime import datetime

def sql_escape(value):
    """Escape single quotes and handle NULLs for SQL."""
    if pd.isna(value):
        return 'NULL'
    return "'" + str(value).replace("'", "''") + "'"


def extract_and_generate_sql(xlsx_file, output_folder, finished_good_item_number):
    try:
        # Load Excel file
        xls = pd.ExcelFile(xlsx_file)

        # Extract info from MASTER sheet
        info_df = pd.read_excel(xls, sheet_name="MASTER - Batch Overview", header=None)
        info_extracted = info_df.iloc[11:20, 1:3]

        if info_extracted.shape[1] != 2:
            raise ValueError(f"Expected 2 columns in 'MASTER - Batch Overview', got {info_extracted.shape[1]}.")

        info_extracted.columns = ['column_name', 'column_value']
        info_dict = dict(zip(info_extracted['column_name'], info_extracted['column_value']))

        # Extract specific values from DATEX sheet
        data_df = pd.read_excel(xls, sheet_name="DATEX", header=None)
        # Get the value from the specific cell
        formula_syrup_gal = data_df.iloc[6, 2]

        # SQL columns
        columns = [
            'finished_goods_item_number',
            'finished_good_brand_company',
            'formula_number',
            'formula_description',
            'finished_good_unit_size',
            'finished_goods_units_per_case',
            'throw_ratio',
            'formula_revision_date',
            'rm_syrup_gallons'
        ]

        # Corresponding values
        values = [
            finished_good_item_number,
            info_dict.get('Brand'),
            info_dict.get('Formula Number'),
            info_dict.get('Formula Description'),
            info_dict.get('Unit Size (oz.)'),
            info_dict.get('Units per Case'),
            info_dict.get('Throw Ratio (Parts Water)'),
            info_dict.get('Formula Revision Date'),
            formula_syrup_gal
        ]

        # Construct SQL statement
        sql_statement = f"INSERT INTO demo_work.finished_goods ({', '.join([f'[{col}]' for col in columns])}) VALUES\n"

        sql_statement += "(" + ", ".join([sql_escape(v) for v in values]) + ");"

        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)

        # Timestamped output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_sql_path = os.path.join(output_folder, f"insert_batch_finished_goods_{timestamp}.sql")

        # Write SQL to file
        with open(output_sql_path, "w", encoding="utf-8") as f:
            f.write(sql_statement)

        success_message = f"Finished_goods SQL generated successfully: {output_sql_path}"
        print(f"✅ {success_message}")
        return True, success_message

    except Exception as e:
        error_message = f"❌ Error generating SQL from {xlsx_file}: {e}"
        print(error_message)
        return False, str(e)
