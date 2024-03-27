pip install 'apache-airflow==2.8.4' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.8.txt"

pip install 'apache-airflow[postgres,mongo, spark]==2.8.3' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.8.txt"

export AIRFLOW_HOME=`pwd`
airflow users create -u admin -p admin -f anhquang -l doan -r Admin -e dev.anhdq@gmail.com