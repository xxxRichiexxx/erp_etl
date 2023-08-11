SELECT
    '{{params.dm}}',
    SUM("Реализовано")||{{task_instance.xcom_pull(key='stage_erp_kit_sales', task_ids='Загрузка_данных_в_stage_слой.stage_erp_kit_sales')}},
    '{{execution_date.date()}}',
    SUM("Реализовано")={{task_instance.xcom_pull(key='stage_erp_kit_sales', task_ids='Загрузка_данных_в_stage_слой.stage_erp_kit_sales')}}
FROM sttgaz.{{params.dm}}
WHERE "Месяц" = '{{execution_date.date().replace(day=1)}}';