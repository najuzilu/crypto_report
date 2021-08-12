#/bin/bash
# brew upgrade && brew install postgresql
conda env create --file ./environment.yml --force
# pip install 'apache-airflow[async,devel,crypto,hdfs,hive,password,postgres,s3]==1.10.15'
# export AIRFLOW_HOME=$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/airflow
