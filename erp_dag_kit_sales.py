
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator

from erp_etl.scripts.collable import etl


source_con = BaseHook.get_connection('erp')
source_username = source_con.login
source_password = quote(source_con.password)
api_endpoint = source_con.host

auth=(source_username, source_password)

column_names = [
        "Counterparty",
        "CounterpartyID",
        "Treaty",
        "TreatyID",
        "ApplicationNo",
        "ApplicationContractingMonth",
        "ShipmentMonth",
        "Equipment",
        "KitDrawingNumber",
        "KitName",
        "DrawingNumberPF",
        "Division",
        "NumberOfKitsInTheApplication",
        "Currency",
        "KitPrice",
        "Discount",
        "DiscountedPackagePrice",
        "ShippedWithinTheSpecifiedPeriod",
        "Completed",
        "TheAmountOfRealtionInPurchasePrices",
        "Revenue",
        "Invoice",
        "Country",
        "CountryKode",
        "PPSDate",
        "Course",
        "NumberOfRealization",
        "NumberRealization",
        "TheAmountOfRealPlacer",
]


dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)


default_args = {
    'owner': 'Швейников Андрей',
    'email': ['xxxRichiexxx@yandex.ru'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'erp_kit_sales',
        default_args=default_args,
        description='Получение данных из ERP. Продажи комплектов.',
        start_date=dt.datetime(2022, 1, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        tasks = []

        for offset in range(0,6):
            tasks.append(
                PythonOperator(
                    task_id=f'get_kit_sales_offset_{offset}',
                    python_callable=etl,
                    op_kwargs={
                        'offset': offset,
                        'data_type': 'stage_erp_kit_sales',
                        'api_endpoint': api_endpoint,
                        'auth': auth,
                        'column_names': column_names,
                        'json_key': 'result',
                        'dwh_engine': dwh_engine,
                        'column_to_check': "ShippedWithinTheSpecifiedPeriod",
                    },
                )
            )

        tasks

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        tables=[
            'dds_erp_counterparty',
            'dds_erp_сountry',
            'dds_erp_division',
        ]

        tasks = []

        for table in tables:
            tasks.append(
                VerticaOperator(
                    task_id=table,
                    vertica_conn_id='vertica',
                    sql=f'scripts/{table}.sql',
                    params={
                        'delta_1': dt.timedelta(days=1),
                        'delta_2': relativedelta(months=-5),
                    }
                )
            )

        kit_sales = VerticaOperator(
            task_id='dds_erp_kit_sales',
            vertica_conn_id='vertica',
            sql='scripts/dds_erp_kit_sales.sql',
            params={
                'delta_1': dt.timedelta(days=1),
                'delta_2': relativedelta(months=-5),
            }
        )

        tasks >> kit_sales

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        dm_erp_kit_sales_v = VerticaOperator(
                    task_id='dm_erp_kit_sales_v',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_erp_kit_sales_v.sql',
                )

        dm_erp_kit_sales_with_classifier_v = VerticaOperator(
                    task_id='dm_erp_kit_sales_with_classifier_v',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_erp_kit_sales_with_classifier_v.sql',
                )
        [dm_erp_kit_sales_v, dm_erp_kit_sales_with_classifier_v]
        
    with TaskGroup('Проверки') as data_checks:

        dm_erp_kit_sales_sheck = VerticaOperator(
                    task_id='dm_erp_kit_sales_sheck',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_erp_kit_sales_sheck.sql',
                    params={
                        'dm': 'dm_erp_kit_sales_v',
                    }
                )
        
        dm_erp_kit_sales_with_classifier = VerticaOperator(
                    task_id='dm_erp_kit_sales_with_classifier_sheck',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_erp_kit_sales_with_classifier_sheck.sql',
                    params={
                        'dm': 'dm_erp_kit_sales_with_classifier_v',
                    }
                )
        
        [dm_erp_kit_sales_sheck, dm_erp_kit_sales_with_classifier]

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
