import pandas as pd
from sdtp import SDMLTable, InvalidDataException
from pathlib import Path
import json


def make_SDMLTable_from_upload(uploaded_file, valid_table_types):
  '''
  Make an SDMLTable with a name from uploaded_file, which
  is a FileStorage object from a Flask request object (see
  https://tedboy.github.io/flask/generated/generated/werkzeug.FileStorage.html)
  Returns a dictionary, {"name", "table"}, where name is the name of the file
  and table is the dictionary form of the SDML Table.  Throws an exception if this isn't a valid
  SDML Table.
  Arguments:
    uploaded_file: an instance of werkzeug.FileStorage
    valid_table_types: a set of table types that can be realized

  '''
  # Error check -- is the file extension .sdml?
  filename = uploaded_file.filename
  if Path(filename).suffix.lower() != '.sdml':
    raise InvalidDataException(f'{filename} must be an SDML file, with a .sdml extension')

  # is the uploaded file's contents a JSON dictionary?
  contents = uploaded_file.read()
  try:
    sdml_form = json.loads(contents)
  except json.JSONDecodeError as e:
    raise InvalidDataException(f'JSON Decode Error {str(e)} when reading {filename}')
  
  # Does the dictionary contain schema and type?
  if type(sdml_form) != dict:
    raise InvalidDataException(f'{filename} is not a dictionary')
  required_keys = {'schema', 'type'}
  missing_keys = set(required_keys) - set(sdml_form.keys())
  if len(missing_keys) > 0:
    raise InvalidDataException(f'{filename} is missing {missing_keys}')
  if not (sdml_form["type"] in valid_table_types):
    raise InvalidDataException(f'The table type of {filename} is {sdml_form["type"]}.  Valid types are {valid_table_types}')
  
  # Make the UploadedSDMLTable with name and dictionary
  return {"name": Path(filename).stem, "table": sdml_form}
