import logging
from datetime import datetime
import os
import subprocess
import requests
import jsonpickle

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from ckanext.mysql2mongodb.data_conv.core.helper import read_package_config, read_database_config
from ckanext.mysql2mongodb.data_conv.core.database_connection import ConvInitOption, ConvOutputOption
from ckanext.mysql2mongodb.data_conv.converter.database.factory import getDatabaseFuntions
from ckanext.mysql2mongodb.data_conv.converter.database.database_function import DatabaseFunctionsOptions

from ckanext.mysql2mongodb.data_conv.converter.converter_factory import ConverterFactory

# Get Airflow Logger
logger = logging.getLogger("airflow.task")

converterFactory = ConverterFactory()


def taskPrepare(**kwargs):
    try:
        # Get context information
        resource_id = kwargs['dag_run'].conf.get('resource_id')
        sql_file_name = kwargs['dag_run'].conf.get('sql_file_name')
        sql_file_url = kwargs['dag_run'].conf.get('sql_file_url')

        # check sql file type
        if sql_file_name.split(".")[1] != "sql":
            logger.error('Invalided MySQL backup file extension!')
            raise ValueError('Invalided MySQL backup file extension!')

        # change dir
        subprocess.run(["whoami"], check=True, shell=True)
        LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        # LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        os.chdir(LOCATION)

        # Read configurations
        db_conf = read_database_config()
        package_conf = read_package_config()
        CKAN_API_KEY = package_conf["X-CKAN-API-Key"]

        # get sql bak
        subprocess.run(
            [f"mkdir -p ./downloads/{resource_id}"], check=True, shell=True)
        subprocess.run([
            f"curl -H \"X-CKAN-API-Key: {CKAN_API_KEY}\" -o ./downloads/{resource_id}/{sql_file_name} {sql_file_url}"], shell=True, check=True)

        # get mysql info
        schema_name = sql_file_name.split(".")[0]
        mysql_host = db_conf["mysql_host"]
        mysql_username = db_conf["mysql_username"]
        mysql_password = db_conf["mysql_password"]
        mysql_port = db_conf["mysql_port"]
        mysql_dbname = schema_name

        source_database_funtions = getDatabaseFuntions(type="MYSQL", options=DatabaseFunctionsOptions(
            host=mysql_host, username=mysql_username, password=mysql_password, port=mysql_port, dbname=mysql_dbname))

        source_database_funtions.restore(
            f"{LOCATION}/downloads/{resource_id}/{sql_file_name}")

        push_to_xcom(kwargs, resource_id, sql_file_name, sql_file_url, db_conf, package_conf, CKAN_API_KEY,
                     schema_name, mysql_host, mysql_username, mysql_password, mysql_port, mysql_dbname)

        return True
    except Exception as exception:
        logger.error("Error occured in taskPrepare Task!")
        logger.error(str(exception))
        raise exception


def taskUploadResult(**kwargs):
    try:
        package_id = kwargs['dag_run'].conf.get('package_id')
        resource_id, sql_file_name, sql_file_url, db_conf, schema_name, mysql_host, mysql_username, mysql_password, mysql_port, mysql_dbname = pull_from_xcom(
            kwargs)

        package_conf = kwargs['ti'].xcom_pull(
            task_ids='taskPrepare', key='package_conf')

        schema_conv_init_option = jsonpickle.decode(kwargs['ti'].xcom_pull(
            task_ids='taskSchemaImport', key='schema_conv_init_option'))

        subprocess.run(["whoami"], check=True, shell=True)
        LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        # LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        os.chdir(LOCATION)

        mongodb_host = db_conf["datastore_host"]
        mongodb_username = db_conf["datastore_username"]
        mongodb_password = db_conf["datastore_password"]
        mongodb_port = db_conf["datastore_port"]
        mongodb_dbname = schema_name
        schema_conv_output_option = ConvOutputOption(
            host=mongodb_host, username=mongodb_username, password=mongodb_password, port=mongodb_port, dbname=mongodb_dbname)

        response = requests.post('http://localhost:5000/api/action/resource_create',
                                 data={"package_id": package_id,
                                       "name": f"{schema_name}-{resource_id}.zip"},
                                 headers={
                                     "X-CKAN-API-Key": package_conf["X-CKAN-API-Key"]},
                                 files={'upload': open(f"./mongodump_files/{schema_name}.zip", 'rb')})

    except Exception as exception:
        logger.error("Error Occured in taskUploadResult Task!")
        logger.error(str(exception))
        raise exception


def taskSchemaImport(**kwargs):
    try:
        source = kwargs['dag_run'].conf.get('source')

        _, _, _, db_conf, schema_name, source_host, source_username, source_password, source_port, source_dbname = pull_from_xcom(
            kwargs)

        subprocess.run(["whoami"], check=True, shell=True)
        LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        # LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        os.chdir(LOCATION)

        schema_conv_init_option = ConvInitOption(
            host=source_host, username=source_username, password=source_password, port=source_port, dbname=source_dbname)

        datastore_host = db_conf["datastore_host"]
        datastore_username = db_conf["datastore_username"]
        datastore_password = db_conf["datastore_password"]
        datastore_port = db_conf["datastore_port"]
        datastore_dbname = schema_name

        schema_conv_output_option = ConvOutputOption(
            host=datastore_host, username=datastore_username, password=datastore_password, port=datastore_port, dbname=datastore_dbname)

        schema_conversion = converterFactory.getSchemaImporter(source)

        schema_conversion.set_config(
            schema_conv_init_option, schema_conv_output_option)
        schema_conversion.run()
        schema_conversion.save()

        kwargs['ti'].xcom_push(key='schema_conv_init_option',
                               value=jsonpickle.encode(schema_conv_init_option))
        kwargs['ti'].xcom_push(key='schema_conv_output_option',
                               value=jsonpickle.encode(schema_conv_output_option))
        kwargs['ti'].xcom_push(key='schema_conversion',
                               value=jsonpickle.encode(schema_conversion))

    except Exception as exception:
        logger.error("Error Occured in taskSchemaImport Task!")
        logger.error(str(exception))
        raise exception


def taskDataImport(**kwargs):
    try:
        source = kwargs['dag_run'].conf.get('source')

        _, _, _, db_conf, schema_name, source_host, source_username, source_password, source_port, source_dbname = pull_from_xcom(
            kwargs)

        schema_conv_output_option = jsonpickle.decode(kwargs['ti'].xcom_pull(
            task_ids='taskSchemaImport', key='schema_conv_output_option'))
        schema_conversion = jsonpickle.decode(kwargs['ti'].xcom_pull(
            task_ids='taskSchemaImport', key='schema_conversion'))

        datastore_host = db_conf["datastore_host"]
        datastore_username = db_conf["datastore_username"]
        datastore_password = db_conf["datastore_password"]
        datastore_port = db_conf["datastore_port"]
        datastore_dbname = schema_name

        subprocess.run(["whoami"], check=True, shell=True)
        LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        # LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        os.chdir(LOCATION)

        schema_conv_init_option = ConvInitOption(
            host=source_host, username=source_username, password=source_password, port=source_port, dbname=source_dbname)

        dataImporter = converterFactory.getDataImporter(source)
        dataImporter.set_config(
            schema_conv_init_option, schema_conv_output_option, schema_conversion)
        dataImporter.run()

    except Exception as exception:
        logger.error("Error Occured in taskDataImport Task!")
        logger.error(str(exception))
        raise exception


def taskSchemaExport(**kwargs):
    try:
        _, _, _, db_conf, schema_name, source_host, source_username, source_password, source_port, source_dbname = pull_from_xcom(
            kwargs)

        subprocess.run(["whoami"], check=True, shell=True)
        LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        # LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        os.chdir(LOCATION)

        schema_conv_init_option = ConvInitOption(
            host=source_host, username=source_username, password=source_password, port=source_port, dbname=source_dbname)

        mongodb_host = db_conf["datastore_host"]
        mongodb_username = db_conf["datastore_username"]
        mongodb_password = db_conf["datastore_password"]
        mongodb_port = db_conf["datastore_port"]
        mongodb_dbname = schema_name

        schema_conv_output_option = ConvOutputOption(
            host=mongodb_host, username=mongodb_username, password=mongodb_password, port=mongodb_port, dbname=mongodb_dbname)

        schema_conversion = converterFactory.getDataImporter(source_dbname)

        schema_conversion.set_config(
            schema_conv_init_option, schema_conv_output_option)
        schema_conversion.run()

        kwargs['ti'].xcom_push(key='schema_conv_init_option',
                               value=jsonpickle.encode(schema_conv_init_option))
        kwargs['ti'].xcom_push(key='schema_conv_output_option',
                               value=jsonpickle.encode(schema_conv_output_option))
        kwargs['ti'].xcom_push(key='schema_conversion',
                               value=jsonpickle.encode(schema_conversion))

    except Exception as exception:
        logger.error("Error Occured in taskSchemaExport Task!")
        logger.error(str(exception))
        raise exception


def taskDataExport(**kwargs):
    try:
        _, _, _, db_conf, schema_name, source_host, source_username, source_password, source_port, source_dbname = pull_from_xcom(
            kwargs)

        schema_conv_output_option = jsonpickle.decode(kwargs['ti'].xcom_pull(
            task_ids='taskSchemaImport', key='schema_conv_output_option'))
        schema_conversion = jsonpickle.decode(kwargs['ti'].xcom_pull(
            task_ids='taskSchemaImport', key='schema_conversion'))

        mongodb_host = db_conf["mongodb_host"]
        mongodb_username = db_conf["mongodb_username"]
        mongodb_password = db_conf["mongodb_password"]
        mongodb_port = db_conf["mongodb_port"]
        mongodb_dbname = schema_name

        subprocess.run(["whoami"], check=True, shell=True)
        LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        # LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        os.chdir(LOCATION)

        schema_conv_init_option = ConvInitOption(
            host=source_host, username=source_username, password=source_password, port=source_port, dbname=source_dbname)

        dataImporter = converterFactory.getDataImporter(source_dbname)
        dataImporter.set_config(
            schema_conv_init_option, schema_conv_output_option, schema_conversion)
        dataImporter.run()

        subprocess.run([f"mkdir -p mongodump_files"], check=True, shell=True)

        destination_database_funtions = getDatabaseFuntions(type="MONGO", options=DatabaseFunctionsOptions(
            host=mongodb_host, username=mongodb_username, password=mongodb_password, port=mongodb_port, dbname=mongodb_dbname))

        destination_database_funtions.backup(
            f"./mongodump_files/{schema_name}")
        os.chdir("./mongodump_files")
        subprocess.run(
            [f"zip -r {schema_name}.zip {schema_name}/*"], check=True, shell=True)

        kwargs['ti'].xcom_push(key='schema_conv_init_option',
                               value=jsonpickle.encode(schema_conv_init_option))

    except Exception as exception:
        logger.error("Error Occured in taskDataExport Task!")
        logger.error(str(exception))
        raise exception


def taskDumpData(**kwargs):
    try:
        _, _, _, db_conf, schema_name, source_host, source_username, source_password, source_port, source_dbname = pull_from_xcom(
            kwargs)

        mongodb_host = db_conf["mongodb_host"]
        mongodb_username = db_conf["mongodb_username"]
        mongodb_password = db_conf["mongodb_password"]
        mongodb_port = db_conf["mongodb_port"]
        mongodb_dbname = schema_name

        subprocess.run(["whoami"], check=True, shell=True)
        LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        # LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        os.chdir(LOCATION)

        schema_conv_init_option = ConvInitOption(
            host=source_host, username=source_username, password=source_password, port=source_port, dbname=source_dbname)

        subprocess.run([f"mkdir -p mongodump_files"], check=True, shell=True)

        destination_database_funtions = getDatabaseFuntions(type="MONGO", options=DatabaseFunctionsOptions(
            host=mongodb_host, username=mongodb_username, password=mongodb_password, port=mongodb_port, dbname=mongodb_dbname))

        destination_database_funtions.backup(
            f"./mongodump_files/{schema_name}")
        os.chdir("./mongodump_files")
        subprocess.run(
            [f"zip -r {schema_name}.zip {schema_name}/*"], check=True, shell=True)

        kwargs['ti'].xcom_push(key='schema_conv_init_option',
                               value=jsonpickle.encode(schema_conv_init_option))

    except Exception as exception:
        logger.error("Error Occured in taskDumpData Task!")
        logger.error(str(exception))
        raise exception


def branch_func(**kwargs):
    target = kwargs['dag_run'].conf.get('target')
    if target == 'MONGO':
        return ['taskSchemaExport', 'taskDataExport', 'taskDumpData']
    else:
        return 'taskDumpData'


def pull_from_xcom(kwargs):
    resource_id = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='resource_id')
    sql_file_name = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='sql_file_name')
    sql_file_url = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='sql_file_url')
    db_conf = kwargs['ti'].xcom_pull(task_ids='taskPrepare', key='db_conf')
    schema_name = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='schema_name')
    mysql_host = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_host')
    mysql_username = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_username')
    mysql_password = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_password')
    mysql_port = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_port')
    mysql_dbname = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_dbname')
    return resource_id, sql_file_name, sql_file_url, db_conf, schema_name, mysql_host, mysql_username, mysql_password, mysql_port, mysql_dbname


def push_to_xcom(kwargs, resource_id, source_file_name, source_file_url, db_conf, package_conf, CKAN_API_KEY, schema_name, source_host, source_username, source_password, source_port, source_dbname):
    kwargs['ti'].xcom_push(key='resource_id', value=resource_id)
    kwargs['ti'].xcom_push(key='sql_file_name', value=source_file_name)
    kwargs['ti'].xcom_push(key='sql_file_url', value=source_file_url)
    kwargs['ti'].xcom_push(key='db_conf', value=db_conf)
    kwargs['ti'].xcom_push(key='package_conf', value=package_conf)
    kwargs['ti'].xcom_push(key='CKAN_API_KEY', value=CKAN_API_KEY)
    kwargs['ti'].xcom_push(key='schema_name', value=schema_name)
    kwargs['ti'].xcom_push(key='mysql_host', value=source_host)
    kwargs['ti'].xcom_push(key='mysql_username', value=source_username)
    kwargs['ti'].xcom_push(key='mysql_password', value=source_password)
    kwargs['ti'].xcom_push(key='mysql_port', value=source_port)
    kwargs['ti'].xcom_push(key='mysql_dbname', value=source_dbname)


dag = DAG('data_conversion', description='Conversion Flow With Import/Export',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

dagTaskPrepare = PythonOperator(task_id='taskPrepare',
                                python_callable=taskPrepare,
                                op_kwargs={},
                                provide_context=True,
                                dag=dag)
dagTaskSchemaImport = PythonOperator(task_id='taskSchemaImport',
                                     python_callable=taskSchemaImport,
                                     op_kwargs={},
                                     provide_context=True,
                                     dag=dag)
dagTaskDataImport = PythonOperator(task_id='taskDataImport',
                                   python_callable=taskDataImport,
                                   op_kwargs={},
                                   provide_context=True,
                                   dag=dag)
dagTaskExport = PythonOperator(task_id='taskExport',
                               python_callable=branch_func,
                               op_kwargs={},
                               provide_context=True,
                               dag=dag)
dagTaskUploadResult = PythonOperator(task_id='taskUploadResult',
                                     python_callable=taskUploadResult,
                                     op_kwargs={},
                                     provide_context=True,
                                     dag=dag)


dagTaskSchemaImport.set_upstream(dagTaskPrepare)
dagTaskDataImport.set_upstream(dagTaskSchemaImport)
dagTaskExport.set_upstream(dagTaskDataImport)
dagTaskUploadResult.set_upstream(dagTaskExport)
