import csv
import sys
sys.path.append('/workspaces/sdtp-data-wiki')
from sdtp import convert_rows_to_type_list
import json
import glob

def convert_csv(rows):
  names = rows[0]
  type_list = rows[1]
  schema = [{"name": names[i], "type": type_list[i]} for i in range(len(names))]
  data_rows = convert_rows_to_type_list(type_list, rows[2:])
  return {
    "schema": schema, "rows": data_rows
  }

def read_file(file_name):
  with open(file_name, 'r') as f:
    rows = csv.reader(f)
    result = [[s.strip() for s in r] for r in rows]
  return result

files = glob.glob('csv/*.csv')
for file in files:
  csv_rows = read_file(file)
  table = convert_csv(csv_rows)
  table_name = file[4:-4]
  sdtp_table = {"name": table_name, "table": table}
  with open(f'tables/{table_name}.json', 'w') as f:
    json_string = json.dumps(sdtp_table)
    f.write(json_string)