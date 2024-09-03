'''
A Sample configuration file for an SDTP Wiki Server.  This should be copied to conf.py and customized in deployment.
It contains three  variables: 
1. SDTP_PATH: the path of directories to search for SDTP Table files.  An SDTP Table File is a .json dictionary with
   entries {schema, type}, and some type-specific entry to get the rows.  The type is an index into the 
   table factories dictionary.
2. TABLE_FACTORIES: A dictionary of TableFactory instances, indexed by table type, a string.  For examples of factories,
   see the factories for the standard tables in sdtp_table.py, specifically `RowTableFactory`.  A TableFactory
   instance has a method, `buildTable(table_dictionary)` which takes in a dictionary with the specifics of the
   table and returns an instance of the appropriate `SDTPTable` subclass.  The dictionary entries _must_ include
   `schema` and `type`, and the value of `type` (a string) must be the index for the Factory in `TABLE_FACTORIES`
3. UPLOAD_FOLDER A folder for uploaded files.  The CSV and XLSX conversion methods rely on pandas, and pandas reads CSV/XLS files, 
   not MIME byte streams.  So we need a place to temporarily store these files. 
'''
BUCKET_NAME = 'berkeley_data_plane_wiki'
TABLE_FACTORIES = {}

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\rick\\.google_keys\\data-plane-428318-efcc7245166a.json"
client_id='561711661897-0dknrvipunasphilaa5ilu9g0o7qrgqn.apps.googleusercontent.com'
client_secret='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'