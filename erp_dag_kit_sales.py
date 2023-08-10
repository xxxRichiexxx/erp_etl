
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
from airflow.operators.python import BranchPythonOperator

from erp_etl.scripts.collable import etl


source_con = BaseHook.get_connection('erp')
source_username = source_con.login
source_password = quote(source_con.password)
source_host = source_con.host
api_endpoint = rf'http://{source_host}/erp_demo/hs/sellers'

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
        start_date=dt.datetime(2023, 7, 1),
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
                    },
                )
            )

        tasks 

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        counterparty = VerticaOperator(
            task_id='dds_erp_counterparty',
            vertica_conn_id='vertica',
            sql='scripts/dds_erp_counterparty.sql',
            params={
                'delta_1': dt.timedelta(days=1),
                'delta_2': relativedelta(months=-5),
            }
        )

        сountry = VerticaOperator(
            task_id='dds_erp_сountry',
            vertica_conn_id='vertica',
            sql='scripts/dds_erp_сountry.sql',
            params={
                'delta_1': dt.timedelta(days=1),
                'delta_2': relativedelta(months=-5),
            }
        )

        division = VerticaOperator(
            task_id='dds_erp_division',
            vertica_conn_id='vertica',
            sql='scripts/dds_erp_division.sql',
            params={
                'delta_1': dt.timedelta(days=1),
                'delta_2': relativedelta(months=-5),
            }
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

        [counterparty, сountry, division] >> kit_sales

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        pass

        # dm_isc_orders_v = VerticaOperator(
        #     task_id='dm_isc_orders_v',
        #     vertica_conn_id='vertica',
        #     sql='scripts/dm_isc_orders_v.sql',
        # )
        
        # dm_isc_contracting_plan = VerticaOperator(
        #     task_id='dm_isc_contracting_plan',
        #     vertica_conn_id='vertica',
        #     sql='scripts/dm_isc_contracting_plan.sql',
        #     params={
        #         'delta_1': dt.timedelta(days=1),
        #         'delta_2': dt.timedelta(days=4),
        #     }
        # )

        # dm_isc_contracting = PythonOperator(
        #     task_id=f'dm_isc_contracting',
        #     python_callable=contracting_calculate,
        #     op_kwargs={
        #         'data_type': 'contracting',
        #         'dwh_engine': dwh_engine,
        #     },
        # )

        # date_check = BranchPythonOperator(
        #     task_id='date_check',
        #     python_callable=date_check,
        #     op_kwargs={
        #         'taskgroup': 'Загрузка_данных_в_dm_слой',
        #         },
        # )

        # do_nothing = DummyOperator(task_id='do_nothing')
        # monthly_tasks = PythonOperator(
        #     task_id='monthly_tasks',
        #     python_callable=contracting_calculate,
        #     op_kwargs={
        #         'data_type': 'contracting',
        #         'dwh_engine': dwh_engine,
        #         'monthly_tasks': True,
        #     },
        # )
        # collapse = DummyOperator(
        #     task_id='collapse',
        #     trigger_rule='none_failed',
        # )

        # [dm_isc_orders_v, dm_isc_contracting_plan] >> dm_isc_contracting >> date_check >> [monthly_tasks, do_nothing] >> collapse
        
    with TaskGroup('Проверки') as data_checks:

        pass

        # dm_isc_orders_v_check = VerticaOperator(
        #             task_id='dm_isc_orders_v_check',
        #             vertica_conn_id='vertica',
        #             sql='scripts/dm_isc_orders_v_check.sql',
        #             params={
        #                 'dm': 'dm_isc_orders_v',
        #             }
        #         )

        # dm_isc_contracting_check = VerticaOperator(
        #             task_id='dm_isc_contracting_check',
        #             vertica_conn_id='vertica',
        #             sql='scripts/dm_isc_contracting_check.sql',
        #             params={
        #                 'dm': 'dm_isc_contracting',
        #             }
        #         )
        
        # [dm_isc_orders_v_check, dm_isc_contracting_check]

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
