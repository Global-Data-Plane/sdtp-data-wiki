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


from flask import  Flask

import sys
import os
from glob import glob
from json import load

'''
This is a simple SDTP Server, designed primarily for illustrative purposes -- this server is configured
by the two variables SDTP_PATH and TABLE_FACTORIES in conf.py.  See the documentation in sample_conf.py.
This is a very thin overlay on the server in sdtp_server.py, with three major extensions:
1. a /, /help method which describes the available routes and required variables.
2. It searches the SDTP_PATH for tables, which are SDTP files
3.   
'''

from conf import SDTP_PATH, TABLE_FACTORIES

sys.path.append('.')
# sys.path.append('./data_plane')
from sdtp import sdtp_server_blueprint
app = Flask(__name__)

app.register_blueprint(sdtp_server_blueprint)

#
# Load any TABLE_FACTORIES before loading any tables, so nonstandard table types are recognized.
# Very simple: just check to make sure that each item in TABLE_FACTORIES is keyed by a string
# and the value is a genuine TableFactory class
#

if TABLE_FACTORIES is not None and type(TABLE_FACTORIES) == dict:
    for (table_type, factory) in TABLE_FACTORIES.items():
        if type(table_type == str) and hasattr(factory, 'build_table'):
            sdtp_server_blueprint.table_server.add_table_factory(table_type, factory)

#
# Load a table.  filename is a valid path and a JSON file.
# 

def _load_table(filename):
    with open(filename, 'r') as fp:
        table_dictionary = load(fp)
        sdtp_server_blueprint.table_server.add_sdtp_table_from_dictionary(table_dictionary)

# 
# Load all the tables on SDTP_PATH.  
#

if SDTP_PATH is not None and len(SDTP_PATH) > 0:
    for path in SDTP_PATH:
        if os.path.exists(path) and os.path.isdir(path):
            files = glob(f'{path}/*.json')
            for filename in files:
                _load_table(filename)


additional_routes = [
     {"url": "/, /help", "headers": "", "method": "GET", "description": "print this message"},
     {"url": "/cwd", "headers": "", "method": "GET", "description": "Show the working directory on the server"},
]

@app.route('/help', methods=['POST', 'GET'])
@app.route('/', methods=['POST', 'GET'])
def show_routes():
    '''
    Show the API for the table server
    Arguments: None
    '''
    pages = sdtp_server_blueprint.ROUTES + additional_routes
    page_strings = [f'<li>{page}</li>' for page in pages]

    return f'<body  style="font-family: Arial, Helvetica, Sans-Serif;"><h1>Supported Methods</h1><ul>{"".join(page_strings)}</ul></body>'



@app.route('/cwd')
def cwd():
    return os.getcwd()


if __name__ == '__main__':
    sdtp_server_blueprint.init_tables()
    app.run()