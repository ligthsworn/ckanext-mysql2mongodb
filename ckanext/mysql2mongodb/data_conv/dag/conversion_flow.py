import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os, requests
from ckanext.mysql2mongodb.data_conv.dag.helper import read_package_config, read_database_config

#Get Airflow Logger
logger = logging.getLogger("airflow.task")

def taskPrepare(**kwargs):
    try:
        resource_id= kwargs['dag_run'].conf.get('resource_id')
        sql_file_name = kwargs['dag_run'].conf.get('sql_file_name')
        sql_file_url = kwargs['dag_run'].conf.get('sql_file_url')

        if sql_file_name.split(".")[1] != "sql":
            logger.error('Invalided MySQL backup file extension!')
            raise ValueError('Invalided MySQL backup file extension!')

        logger.error("2")
        os.system("whoami")
        # LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        os.chdir(LOCATION)

        os.system(f"mkdir -p ./downloads/{resource_id}")
        os.system(f"curl -o ./downloads/{resource_id}/{sql_file_name} {sql_file_url}")

        db_conf = read_database_config()
        package_conf = read_package_config()
        logger.error("3")

        schema_name = sql_file_name.split(".")[0]

        mysql_host = db_conf["mysql_host"]
        mysql_username = db_conf["mysql_username"]
        mysql_password = db_conf["mysql_password"]
        mysql_port = db_conf["mysql_port"]
        mysql_dbname = schema_name
        logger.error("4")


        mysql_conn = open_connection_mysql(mysql_host, mysql_username, mysql_password)
        mysql_cur = mysql_conn.cursor()
        mysql_cur.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_dbname};")
        mysql_cur.close()
        mysql_conn.close()

        os.system(f"mysql -h {mysql_host} -u {mysql_username} --password={mysql_password} {schema_name} < ./downloads/{resource_id}/{sql_file_name}")
        logger.error("Task finished!")
        return True
    except Exception as exception:
        logger.error("Error Occure in taskPrepare Task!")
        logger.error(str(exception))
        raise exception

def taskSchemaConv(**kwargs):
    try:
        resource_id= kwargs['dag_run'].conf.get('resource_id')
        sql_file_name = kwargs['dag_run'].conf.get('sql_file_name')
        sql_file_url = kwargs['dag_run'].conf.get('sql_file_url')

        schema_conv_init_option = ConvInitOption(host = mysql_host, username = mysql_username, password = mysql_password, port = mysql_port, dbname = mysql_dbname)

        mongodb_host = db_conf["mongodb_host"]
        mongodb_username = db_conf["mongodb_username"]
        mongodb_password = db_conf["mongodb_password"]
        mongodb_port = db_conf["mongodb_port"]
        mongodb_dbname = schema_name
        schema_conv_output_option = ConvOutputOption(host = mongodb_host, username = mongodb_username, password = mongodb_password, port = mongodb_port, dbname = mongodb_dbname)

        schema_conversion = SchemaConversion()
        schema_conversion.set_config(schema_conv_init_option, schema_conv_output_option)
        schema_conversion.run()

    except Exception as exception:
        logger.error("Error Occure in taskSchemaConv Task!")
        logger.error(str(exception))
        raise exception

def taskDataConv(**kwargs):
    try:
        resource_id= kwargs['dag_run'].conf.get('resource_id')
        sql_file_name = kwargs['dag_run'].conf.get('sql_file_name')
        sql_file_url = kwargs['dag_run'].conf.get('sql_file_url')

        mysql2mongodb = DataConversion()
        mysql2mongodb.set_config(schema_conv_init_option, schema_conv_output_option, schema_conversion)
        mysql2mongodb.run()

        os.system(f"mkdir -p mongodump_files")
        os.system(f"mongodump --username {mongodb_username} --password {mongodb_password} --host {mongodb_host} --port {mongodb_port} --authenticationDatabase admin --db {mongodb_dbname} -o mongodump_files/")
        os.chdir("./mongodump_files")
        os.system(f"zip -r {schema_name}.zip {schema_name}/*")
    except Exception as exception:
        logger.error("Error Occure in taskDataConv Task!")
        logger.error(str(exception))
        raise exception

def taskUploadResult(**kwargs):
    try:
        resource_id= kwargs['dag_run'].conf.get('resource_id')
        sql_file_name = kwargs['dag_run'].conf.get('sql_file_name')
        sql_file_url = kwargs['dag_run'].conf.get('sql_file_url')

        response = requests.post('http://localhost:5000/api/action/resource_create',
            data={"package_id":package_conf["package_id"], "name":f"{schema_name}-{resource_id}.zip"},
            headers={"X-CKAN-API-Key": package_conf["X-CKAN-API-Key"]},
            files={'upload': open(f"{schema_name}.zip", 'rb')})
    except Exception as exception:
        logger.error("Error Occure in taskUploadResult Task!")
        logger.error(str(exception))
        raise exception


dag = DAG('conversion_flow', description='Basic Conversion Flow',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

task1 = PythonOperator(task_id='taskPrepare',
                        python_callable=taskPrepare,
                        op_kwargs= {},
                        provide_context=True,
                        dag = dag)
task2 = PythonOperator(task_id='taskSchemaConv',
                        python_callable=taskSchemaConv,
                        op_kwargs= {},
                        provide_context=True,
                        dag = dag)

task3 = PythonOperator(task_id='taskDataConv',
                        python_callable=taskDataConv,
                        op_kwargs= {},
                        provide_context=True,
                        dag = dag)
task4 = PythonOperator(task_id='taskUploadResult',
                        python_callable=taskUploadResult,
                        op_kwargs= {},
                        provide_context=True,
                        dag = dag)
task2.set_upstream(task1)
task3.set_upstream(task2)
task4.set_upstream(task3)
