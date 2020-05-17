"""PROMINENCE REST API"""
import logging
import os
import sys
from flask import Flask

from accounting import accounting
from data import data
from jobs import jobs
from workflows import workflows

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

if __name__ == "__main__":
    if 'PROMINENCE_RESTAPI_CONFIG_FILE' not in os.environ:
        logging.error('Environment variable PROMINENCE_RESTAPI_CONFIG_FILE has not been defined, exiting')
        exit(1)

    app = Flask(__name__)
    app.config.from_pyfile(os.environ['PROMINENCE_RESTAPI_CONFIG_FILE'])
    app.register_blueprint(accounting)
    app.register_blueprint(data)
    app.register_blueprint(jobs)
    app.register_blueprint(workflows)
    app.run()
