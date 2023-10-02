This is an orchistraged solution to run on Airflow could - Astronomer. You can just run it locally using just the Apache Airflfow 

Successful installation requires a Python 3 environment. Starting with Airflow 2.3.0, Airflow is tested with Python 3.8, 3.9, 3.10. Note that Python 3.11 is not yet supported.
Only pip installation is currently officially supported.


1) Set Airflow Home (optional):

`export AIRFLOW_HOME=~/airflow`

AIRFLOW_VERSION=2.7.1


2) install airlfow

`PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"`

`CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"`

`pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"`


3) Run airflow standalone:

`airflow standalone`


4) Access the Airflow UI:
Visit `localhost:8080` in your browser and log in with the admin account details shown in the terminal. Enable the example_bash_operator DAG in the home page.

