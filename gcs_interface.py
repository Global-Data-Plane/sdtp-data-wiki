from google.cloud import storage
import json


class SDMLStorageBucket:
  '''
  An object which is the interface to the Google Cloud Storage Bucket holding SDML files 
  as blobs.  The SDML files should all end with '.sdml' and be valid tables
  '''
  def __init__(self, bucket_name):
    self.bucket_name = bucket_name
    self.client = storage.Client()
    self.bucket = self.client.bucket(bucket_name)

  def get_all_table_names(self):
    '''
    Get the names of all tables stored in the bucket.  Returns a list of blob names
    '''
    blobs = self.client.list_blobs(self.bucket)
    names = [blob.name for blob in blobs]
    return [name for name in names if name.endswith('.sdml')]
  
  def get_table_as_dictionary(self, table_name):
    '''
    Get table table_name as the dictionary form.  The SDML blob in the bucket should be 
    an SDML file in JSON form
    Arguments: 
      table_name: the name of the table to get
    Returns:
      The table as a JSON dictionary
    '''
    blob = self.bucket.blob(table_name)
    json_form = blob.download_as_string()
    result  = json.loads(json_form)
    return result
  
  def upload_table(self, table_dictionary):
    '''
    Upload a table to the bucket, to blob name table_name.  table should be an SDMLTable with 
    a .to_dictionary() method that produces a JSONifiable form
    Arguments:
      table_name: name of the table
      table: the SDML Table; note this is an instance of SDMLTable, not a dictionary
    
    '''
    table_name = table_dictionary["name"]

    blob = self.bucket.blob(f'{table_name}.sdml')
    json_form = json.dumps(table_dictionary['table'])
    blob.upload_from_string(json_form, content_type = 'application/json')



