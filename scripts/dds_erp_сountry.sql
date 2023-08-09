
INSERT INTO sttgaz.dds_erp_сountry
("Страна", "Код страны", ts)
WITH 
    sq AS(
        SELECT DISTINCT HASH("Страна", "Код страны")
        FROM sttgaz.dds_erp_сountry
    )
SELECT DISTINCT
    "Country",
    "CountryKode",
    NOW()
FROM sttgaz.stage_erp_kit_sales AS s
WHERE DATE_TRUNC('month', TO_DATE("ShipmentMonth", 'DD:MM:YYYY'))::date IN (
        '{{execution_date.replace(day=1)}}',
        '{{(execution_date.replace(day=1) - params.delta_1).replace(day=1)}}'
    )
    AND HASH(s."Country", s."CountryKode") NOT IN (SELECT * FROM sq);
