"""Top-level package for the Simple Data Transfer Protocol."""


# BSD 3-Clause License

# Copyright (c) 2023, engageLively
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.

# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.

# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from flask import  Flask, request, render_template, flash, redirect, url_for, session

import sys
import os
from authlib.integrations.flask_client import OAuth
import jwt
from uploader import make_SDMLTable_from_upload


from sdtp import sdtp_server_blueprint
from wiki_server import wiki_server, show_root
app = Flask(__name__)

app.register_blueprint(wiki_server)

app.register_blueprint(sdtp_server_blueprint)

# from google.cloud import bigquery

app.secret_key = 'your_secret_key_here'
# root = 'https://data-plane-428318.uw.r.appspot.com/'
root = 'http://localhost:8080'

from conf import client_id, client_secret
# Configure OAuth
oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=client_id,
    client_secret=client_secret,
    authorize_url='https://accounts.google.com/o/oauth2/auth',
    access_token_url='https://accounts.google.com/o/oauth2/token',
    redirect_uri=f'{root}/oauth2callback',
    client_kwargs={'scope': 'email'},
    server_metadata_url = 'https://accounts.google.com/.well-known/openid-configuration'
)

@app.route('/login')
def login():
    redirect_uri = url_for('authorize', _external=True)
    return google.authorize_redirect(redirect_uri)

@app.route('/oauth2callback')
def authorize():
    try:
        # Exchange the authorization code for an access token
        token = google.authorize_access_token()
        
        id_token = token.get('id_token')
        user_info = jwt.decode(id_token, options={"verify_signature": False})
        email = user_info.get('email')
        session['email'] = email
        session['user'] = email.split('@')[0]
        return show_root()
    except Exception as e:
        # Capture and return the error message
        return f'Error: {str(e)}'
# client = bigquery.Client()

@app.route('/cwd')
def cwd():
    return os.getcwd()

from build_filter import create_filter    
from sdtp import check_valid_spec_return_boolean, InvalidDataException

@app.route('/view_table')
def view_table():
    table_name = request.args.get('table')
    filter = request.args.get('filter')
    table = sdtp_server_blueprint.table_server.get_table(table_name)
    sdtp_filter_spec = create_filter(filter)
    if not check_valid_spec_return_boolean(sdtp_filter_spec):
        sdtp_filter_spec = None
        filter = ''

    rows = table.get_filtered_rows(sdtp_filter_spec)
    if len(rows) > 20: rows = rows[:20]
    return render_template('table.html',
                           filter = filter if filter is not None else '',
                           table = {
                               "name": table_name, "columns": table.schema, "rows": rows
                            })

from conf import BUCKET_NAME

from gcs_interface import SDMLStorageBucket

bucket = SDMLStorageBucket(BUCKET_NAME)

def _check_email():
    # A  utility to ensure that only registered users 
    # can upload files
    return 'email' in session and session['email'].endswith('berkeley.edu')

def upload_error(message):
    flash(message)
    return redirect(request.url)


@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if not _check_email():
        flash('Uploads restricted to individuals with a berkeley.edu account')
        return show_root()
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            # flash('No selected file')
            return redirect(request.url)
        # make_SDMLTable_from_upload checks to maek sure it's an SDML file
        try:
            table_types = set(sdtp_server_blueprint.table_server.factories.keys())
            table_dictionary = make_SDMLTable_from_upload(request.files['file'], table_types)  
        except InvalidDataException as e:
            flash(str(e))
            return redirect(request.url)
        try:
            table_dictionary["name"] = f"{session['user']}/{table_dictionary['name']}"
            sdtp_server_blueprint.table_server.add_sdtp_table_from_dictionary(table_dictionary["name"], table_dictionary["table"])
        except InvalidDataException as e:
            return upload_error(f'Error {e} in creating the table for  {file.filename}')
        bucket.upload_table(table_dictionary)
        return redirect(f"/view_table?table={table_dictionary['name']}")
        
       
    return render_template('upload_form.html')
    

@app.route("/view_tables")
def view_tables():
    table_names = sdtp_server_blueprint.table_server.servers.keys()
    return render_template('view_tables.html', tables=table_names)

@app.route("/view_base")
def view_base():
    return render_template('base.html')

additional_routes =[
    {"url": "/upload", "method": ["GET", "POST"], "description": "File uploader.  If a multipart file is not attached to the POST body, displays a file chooser"},
    {"url": "/view_tables", "method": ["GET"], "description": "Shows all the tables in a list, with a link to the table viewer method"},
    {"url": "/view_table?table <i>string, required</i>&filter<i>string, optional</i>", "method": ["GET", "POST"], "description": "Table Viewer.  Displays the first twenty rows of the table (filtered, if filter was applied).  filter, if present is a functional filter expression, e.g. IN_RANGE('<column_name>, <min_val>, <max_val>)"},
    {"url": "/view_base", "method": ["GET", "POST"], "description": "Check out the base template"}
]

@app.route('/help', methods=['POST', 'GET'])
@app.route('/', methods=['POST', 'GET'])
def show_routes():
    '''
    Show the API for the table server
    Arguments: None
    '''
    pages = sdtp_server_blueprint.ROUTES + additional_routes
    keys = ['url', 'method', 'headers', 'description']
    for page in pages:
        for key in keys:
            if not key in page.keys():
                page[key] = ''

    return render_template('routes.html', pages = pages, keys = keys)


table_names = bucket.get_all_table_names()
for table_name in table_names:
    table_dict = bucket.get_table_as_dictionary(table_name)
    sdtp_server_blueprint.table_server.add_sdtp_table_from_dictionary(table_name, table_dict)


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))