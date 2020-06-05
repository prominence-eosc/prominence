"""PROMINENCE REST API"""
import logging
import os
import sys
from flask import Flask

from prominence import accounting
from prominence import data
from prominence import jobs
from prominence import workflows

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

app = Flask(__name__)
app.config.from_pyfile(os.environ['PROMINENCE_RESTAPI_CONFIG_FILE'])
app.register_blueprint(accounting.accounting)
app.register_blueprint(data.data)
app.register_blueprint(jobs.jobs)
app.register_blueprint(workflows.workflows)

if __name__ == "__main__":
    if 'PROMINENCE_RESTAPI_CONFIG_FILE' not in os.environ:
        logging.error('Environment variable PROMINENCE_RESTAPI_CONFIG_FILE has not been defined, exiting')
        exit(1)

    app.run()
