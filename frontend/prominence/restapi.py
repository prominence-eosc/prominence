"""PROMINENCE REST API"""
import logging
import os
import sys
from flask import Flask

from accounting import accounting
from data import data
from jobs import jobs
from workflows import workflows

# Check required environment variable
if 'PROMINENCE_RESTAPI_CONFIG_FILE' not in os.environ:
    print('ERROR: Environment variable PROMINENCE_RESTAPI_CONFIG_FILE has not been defined')
    exit(1)

# Logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Setup Flask
app = Flask(__name__)
app.config.from_pyfile(os.environ['PROMINENCE_RESTAPI_CONFIG_FILE'])
app.register_blueprint(accounting)
app.register_blueprint(data)
app.register_blueprint(jobs)
app.register_blueprint(workflows)

if __name__ == "__main__":
    app.run()
