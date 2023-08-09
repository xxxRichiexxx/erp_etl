
INSERT INTO sttgaz.dds_erp_division
("Наименование", ts)
WITH 
    sq AS(
        SELECT DISTINCT "Наименование"
        FROM sttgaz.dds_erp_division
    )
SELECT DISTINCT
    "Division",
    NOW()
FROM sttgaz.stage_erp_kit_sales AS s
WHERE (DATE_TRUNC('month', "load_date")::date BETWEEN
        '{{execution_date.replace(day=1) + params.delta_2}}'
        AND '{{execution_date.replace(day=1)}}')
    AND s."Division" NOT IN (SELECT * FROM sq);
