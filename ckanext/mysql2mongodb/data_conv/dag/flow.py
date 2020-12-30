import codecs
import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from pprint import pprint
import urllib, json, re, os, requests

from helper import read_package_config, read_database_config


logging.basicConfig(format="%(name)s-%(levelname)s-%(asctime)s-%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
schema_name = ""
db_conf = ""
package_conf = ""

def create_dag(dag_id, resource_id, sql_file_name, sql_file_url):
    default_args = {
        "owner": "jyoti",
        "description": (
            "DAG to explain airflow concepts"
        ),
        "depends_on_past": False,
        "start_date": dates.days_ago(1),
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "provide_context": True,
    }

    new_dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=timedelta(minutes=5),
    )

    def taskPrepare(resource_id, sql_file_name, sql_file_url):
        pprint("Start conversion!")
        if sql_file_name.split(".")[1] != "sql":
            print("Invalided MySQL backup file extension!")
        os.system("whoami")
        os.chdir("/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv")

        os.system(f"mkdir -p ./downloads/{resource_id}")
        os.system(f"curl -o ./downloads/{resource_id}/{sql_file_name} {sql_file_url}")

        db_conf = read_database_config()
        package_conf = read_package_config()

        schema_name = sql_file_name.split(".")[0]

        mysql_host = db_conf["mysql_host"]
        mysql_username = db_conf["mysql_username"]
        mysql_password = db_conf["mysql_password"]
        mysql_port = db_conf["mysql_port"]
        mysql_dbname = schema_name

        mysql_conn = open_connection_mysql(mysql_host, mysql_username, mysql_password)
        mysql_cur = mysql_conn.cursor()
        mysql_cur.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_dbname};")
        mysql_cur.close()
        mysql_conn.close()

        os.system(f"mysql -h {mysql_host} -u {mysql_username} --password={mysql_password} {schema_name} < ./downloads/{resource_id}/{sql_file_name}")

    def taskSchemaConv(resource_id, sql_file_name, sql_file_url):

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

    def taskDataConv(resource_id, sql_file_name, sql_file_url):
        mysql2mongodb = DataConversion()
        mysql2mongodb.set_config(schema_conv_init_option, schema_conv_output_option, schema_conversion)
        mysql2mongodb.run()

        os.system(f"mkdir -p mongodump_files")
        os.system(f"mongodump --username {mongodb_username} --password {mongodb_password} --host {mongodb_host} --port {mongodb_port} --authenticationDatabase admin --db {mongodb_dbname} -o mongodump_files/")
        os.chdir("./mongodump_files")
        os.system(f"zip -r {schema_name}.zip {schema_name}/*")

    def taskUploadResult(resource_id, sql_file_name, sql_file_url):
        response = requests.post('http://localhost:5000/api/action/resource_create',
                data={"package_id":package_conf["package_id"], "name":f"{schema_name}-{resource_id}.zip"},
                headers={"X-CKAN-API-Key": package_conf["X-CKAN-API-Key"]},
                files={'upload': open(f"{schema_name}.zip", 'rb')})

        pprint(response.content)

        pprint("Done!")

    with new_dag:
        task1 = PythonOperator(task_id='taskPrepare',
                                                python_callable=taskPrepare,
                                                op_kwargs=
                                                    {
                                                        'resource_id': resource_id,
                                                        'sql_file_name': sql_file_name,
                                                        'sql_file_url' : sql_file_url
                                                    },
                                                    provide_context=True)
        task2 = PythonOperator(task_id='taskSchemaConv',
                                            python_callable=taskSchemaConv,
                                            op_kwargs=
                                                    {
                                                        'resource_id': resource_id,
                                                        'sql_file_name': sql_file_name,
                                                        'sql_file_url' : sql_file_url
                                                    },
                                            provide_context=True)

        task3 = PythonOperator(task_id='taskDataConv',
                                            python_callable=taskDataConv,
                                            op_kwargs=
                                                    {
                                                        'resource_id': resource_id,
                                                        'sql_file_name': sql_file_name,
                                                        'sql_file_url' : sql_file_url
                                                    },
                                            provide_context=True)
        task4 = PythonOperator(task_id='taskUploadResult',
                                            python_callable=taskUploadResult,
                                            op_kwargs=
                                                    {
                                                        'resource_id': resource_id,
                                                        'sql_file_name': sql_file_name,
                                                        'sql_file_url' : sql_file_url
                                                    },
                                            provide_context=True)
        task2.set_upstream(task1)
        task3.set_upstream(task2)
        task4.set_upstream(task3)

        return new_dag
