#/bin/bash init_db.sh
CWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
who_am_i=$(whoami)
brew services start postgresql
sudo -u $who_am_i bash -c "psql < ${CWD}/setup_db.sql"
airflow db init
airflow connections delete airflow_db
airflow connections add airflow_db --conn-uri postgres://airflow:airflow@localhost:5432/airflow
