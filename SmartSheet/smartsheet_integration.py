import smartsheet
import os
import pandas as pd

_dir = os.path.dirname(os.path.abspath(__file__))

smart = smartsheet.Smartsheet(" ")
excel_data = "Sample Sheet.xlsx"
import_status_sheet_id = smart.Sheets.get_sheet(" ")
# Import the sheet
result = smart.Sheets.import_xlsx_sheet(_dir + '/Sample Sheet.xlsx', header_row_index=0)

dataframe = pd.read_excel(excel_data)

column_map = {column.title: column.id for column in import_status_sheet_id.columns}
print(column_map)
rows_to_update = []

for index, row in dataframe.iterrows():
    # Assuming 'ID' is the unique identifier column
    record_id = row['Row ID']
    for smartsheet_row in import_status_sheet_id.rows:
        # Check if the 'ID' matches
        if smartsheet_row.cells[0].value == record_id:  # Assuming the first cell is the 'ID' column
            cells_to_update = []
            #print(smartsheet_row.cells[0].value)
            for column_title, column_id in column_map.items():
                new_cell = smart.models.Cell()
                new_cell.column_id = column_id
                new_cell.value = row[column_title]
                cells_to_update.append(new_cell)
            new_row = smart.models.Row()
            new_row.id = smartsheet_row.id
            new_row.cells = cells_to_update
            rows_to_update.append(new_row)
            break  # Move to the next row in the DataFrame

smart.Sheets.update_rows(import_status_sheet_id.id, rows_to_update)