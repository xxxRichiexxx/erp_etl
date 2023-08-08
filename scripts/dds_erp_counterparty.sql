
INSERT INTO sttgaz.dds_erp_counterparty
("CounterpartyID", "Контрагент", ts)
WITH 
    sq AS(
        SELECT DISTINCT "CounterpartyID"
        FROM sttgaz.dds_erp_counterparty
    )
SELECT DISTINCT
    "CounterpartyID",
    "Counterparty",
    NOW()
FROM sttgaz.stage_erp_kit_sales AS s
WHERE DATE_TRUNC('month', TO_DATE("ShipmentMonth", 'DD:MM:YYYY'))::date IN (
        '{{execution_date.replace(day=1)}}',
        '{{(execution_date.replace(day=1) - params.delta).raplace(day=1)}}'
    )
    AND s."CounterpartyID" NOT IN (SELECT * FROM sq);
