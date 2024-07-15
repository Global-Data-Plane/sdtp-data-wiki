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


from flask import  Flask, request, render_template, flash, redirect

import sys
import os
from json import dump

sys.path.append('.')
# sys.path.append('./data_plane')
from sdtp import sdtp_server_blueprint, SDTPTable, InvalidDataException, jsonifiable_column, SDTPFilter, jsonifiable_rows, DataFrameTable, Table
app = Flask(__name__)

app.register_blueprint(sdtp_server_blueprint)

# from google.cloud import bigquery
import pandas as pd


# client = bigquery.Client()



@app.route('/cwd')
def cwd():
    return os.getcwd()

from build_filter import create_filter    
from sdtp import check_valid_spec_return_boolean

@app.route('/view_table')
def view_table():
    table_name = request.args.get('table')
    filter = request.args.get('filter')
    table = sdtp_server_blueprint.table_server.get_table(table_name)
    sdtp_filter_spec = create_filter(filter)
    if not check_valid_spec_return_boolean(sdtp_filter_spec):
        sdtp_filter_spec = None
        filter = ''

    rows =   table.get_filtered_rows(sdtp_filter_spec)
    if len(rows) > 20: rows = rows[:20]
    return render_template('table.html',
                           filter = filter if filter is not None else '',
                           table = {
                               "name": table_name, "columns": table.schema, "rows": rows
                            })




UPLOAD_FOLDER = '/tmp'


from uploader import UploadedFile

    
@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
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
        upload_info = UploadedFile(file.filename)
        if  upload_info.type:
            saved_file = os.path.join(UPLOAD_FOLDER, upload_info.safe_filename())
            file.save(saved_file)
            new_table = upload_info.convert(saved_file)
            if new_table:
                table_name = upload_info.base
                table_file_name = os.path.join(sdtp_server_blueprint.sdtp_path[0], upload_info.table_filename())
                with open(table_file_name, "w") as table_file:
                    table_descriptor = new_table.to_dictionary()
                    full_descriptor = {"name": table_name, "table": table_descriptor}
                    dump(full_descriptor, table_file)
                stored_table = Table(new_table)
                sdtp_server_blueprint.table_server.add_sdtp_table({"name": table_name, "table": stored_table})
                return redirect(f'/view_table?table={table_name}')
            else:
                return f'bad file type {upload_info.type}'
    return render_template('upload_form.html')
    

@app.route("/view_tables")
def view_tables():
    table_names = sdtp_server_blueprint.table_server.servers.keys()
    return render_template('view_tables.html', tables=table_names)



sdtp_server_blueprint.configure({
    'sdtp_path': [os.path.join(os.getcwd(), 'tables')],
    'additional_routes': [
        {"url": "/upload", "method": ["GET", "POST"], "description": "File uploader.  If a multipart file is not attached to the POST body, displays a file chooser"},
        {"url": "/view_tables", "method": ["GET"], "description": "Shows all the tables in a list, with a link to the table viewer method"},
        {"url": "/view_table?table<i>string, required</i>&filter<i>string, optional</i>", "method": ["GET", "POST"], "description": "Table Viewer.  Displays the first twenty rows of the table (filtered, if filter was applied).  filter, if present is a functional filter expression, e.g. IN_RANGE('<column_name>, <min_val>, <max_val>)"}
    ],
    'additional_factories': [] # if present, should be a list of the form (<type>, <factory_instance>)
})

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))