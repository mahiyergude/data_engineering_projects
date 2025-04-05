"""
A Flask Application for Deploying DBT
"""
import os
import logging
import json
import os

from flask import Flask, request, render_template
import google.cloud.logging
from dbt.cli.main import dbtRunner, dbtRunnerResult


client = google.cloud.logging.Client()
client.setup_logging()

# pylint: disable=C0103
app = Flask(__name__)


@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    message = "It's running!"

    """Get Cloud Run environment variables."""
    service = os.environ.get('K_SERVICE', 'Unknown service')
    revision = os.environ.get('K_REVISION', 'Unknown revision')

    logging.info(f'Service: {service}, Revision: {revision}')

    return render_template('index.html',
        message=message,
        Service=service,
        Revision=revision)


@app.route('/run_transformation', methods=['POST'])
def run_transformation():
    """DBT run_transformation Runner."""

    try:

        json = request.get_json(force=True) # https://stackoverflow.com/questions/53216177/http-triggering-cloud-function-with-cloud-scheduler/60615210#60615210

        # initialize
        dbt = dbtRunner()

        # create CLI args as a list of strings
        cli_args = ["--project-dir", "dbt", "--profiles-dir", "dbt"]
 
        logging.info('Running: dbt source freshness')
        res: dbtRunnerResult = dbt.invoke(['source', 'freshness'] + cli_args)
        # Add handle_res() function to handle the results

        logging.info('Running: dbt build')
        res: dbtRunnerResult = dbt.invoke(['build'] + cli_args)
        # Add handle_res() function to handle the results

        if res.success==True:
            msg = 'DBT Run Successfully'
            logging.info(msg)
        else:
            msg = "Error in DBT run, check the logs"
            logging.exception(msg)
            raise Exception(msg)
        return msg    
    
    except Exception as e:
        logging.exception(e)
        raise Exception(e)


if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')