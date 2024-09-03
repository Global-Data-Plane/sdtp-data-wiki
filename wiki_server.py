from flask import Blueprint, render_template, session
from sdtp import sdtp_server_blueprint

wiki_server = Blueprint('sdtp_wiki_server', __name__, template_folder='templates')

additional_routes =[
  {"url": "/upload", "method": ["GET", "POST"], "description": "File uploader.  If a multipart file is not attached to the POST body, displays a file chooser"},
  {"url": "/view_tables", "method": ["GET"], "description": "Shows all the tables in a list, with a link to the table viewer method"},
  {"url": "/view_table?table <i>string, required</i>&filter<i>string, optional</i>", "method": ["GET", "POST"], "description": "Table Viewer.  Displays the first twenty rows of the table (filtered, if filter was applied).  filter, if present is a functional filter expression, e.g. IN_RANGE('<column_name>, <min_val>, <max_val>)"},
  {"url": "/view_base", "method": ["GET", "POST"], "description": "Check out the base template"}
]

@wiki_server.route('/')
def show_root():
  '''
  Show the API for the table server
  Arguments: None
  '''
  
  routes = sdtp_server_blueprint.ROUTES
  table_names = list(sdtp_server_blueprint.table_server.servers.keys())
  if 'email' in session:
    return render_template('greeting.html', routes = routes, table_names = table_names, additional_routes = additional_routes, email = session['email'])
  else:
    return render_template('greeting.html', routes = routes, additional_routes = additional_routes, table_names = table_names)


