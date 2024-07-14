import pandas as pd
from sdtp import DataFrameTable

ALLOWED_EXTENSIONS = {'csv': 'csv',  'xls': 'excel', 'xlsx': 'excel'}
from werkzeug.utils import secure_filename

def _clean_row(row):
  return [s.strip() if type(s) == str else s for s in row]

def _convert_dataframe(df):
  all_rows = df.values.tolist()
  rows = [_clean_row(r) for r in all_rows[1:]]
  types = _clean_row(all_rows[0])
  schema = [{"name": df.columns[i], "type": types[i]} for i in range(len(types))]
  df2 = pd.DataFrame(rows, columns=df.columns)
  return DataFrameTable(schema, df2)

def convert_excel(excel_file):
  df = pd.read_excel(excel_file)
  return _convert_dataframe(df)

def convert_csv(csv_file):
  df = pd.read_csv(csv_file)
  return _convert_dataframe(df)

class UploadedFile:
    def __init__(self, filename):
        try:
            parts = filename.rsplit('.', 1)
            self.suffix = parts[1]
            self.base = parts[0]
            key = parts[1].lower()
            self.type = ALLOWED_EXTENSIONS[key] if key in ALLOWED_EXTENSIONS.keys() else None
        except Exception:
            self.suffix = self.base = self.type = None
    
    def table_filename(self):
        return secure_filename(f'{self.base}.json')
    
    def safe_filename(self):
        return secure_filename(f'{self.base}.{self.suffix}')
    
    def convert(self, filename):
        if self.type == 'csv':
            return convert_csv(filename)
        elif self.type == 'excel':
            return convert_excel(filename)
        else:
            return None