INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
    '{{params.dm}}',
    SUM("Реализовано")||'; {{task_instance.xcom_pull(key='stage_erp_kit_sales', task_ids='Загрузка_данных_в_stage_слой.get_kit_sales_offset_0')}}',
    '{{execution_date.date()}}',
    SUM("Реализовано")={{task_instance.xcom_pull(key='stage_erp_kit_sales', task_ids='Загрузка_данных_в_stage_слой.get_kit_sales_offset_0')}}
FROM sttgaz.{{params.dm}}
WHERE "Месяц" = '{{execution_date.date().replace(day=1)}}';