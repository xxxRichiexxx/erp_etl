SELECT DROP_PARTITIONS(
    'sttgaz.dds_erp_kit_sales',
    '{{(execution_date.replace(day=1) - params.delta).raplace(day=1)}}',
    '{{execution_date.replace(day=1)}}',
);

INSERT INTO sttgaz.dds_erp_kit_sales
(
	"Контрагент ID",
        "Договор",
        "Договор ID",
        "Страна ID",
        "Номер приложения",
        "Месяц контрактации",
        "Месяц отгрузки",
        "Комплектация (вариант сборки)",
        "Чертежный номер комплекта",
        "Наименование комплекта",
        "Чертежный номер полуфабриката кабины",
        "Количество комплектов в приложении",
        "Валюта. Код",
        "Цена комплекта",
        "Скидка (процент)", 
        "Цена комплекта с учетом скидки",
        "Отгружено за указанный период",
        "Процент выполнения",
        "Сумма реал-ции в приходных ценах, руб.",
        "Выручка",
        "Счет-фактура Номер",
        "Валюта. Курс",
        "Счет-фактура Дата",
        "Торг12 Номер",
        "TheAmountOfRealPlacer"
)
SELECT
        c.id,
        "Treaty",
        "TreatyID",
        cnt.id
        "ApplicationNo",
        TO_DATE("ApplicationContractingMonth", 'DD:MM:YYYY'),
        TO_DATE("ShipmentMonth", 'DD:MM:YYYY'),
        "Equipment",
        "KitDrawingNumber",
        "KitName",
        "DrawingNumberPF",
        REGEXP_REPLACE("NumberOfKitsInTheApplication",  '\p{Z}', '')::int,  --------------
        "Currency",
        REPLACE(REGEXP_REPLACE("KitPrice",  '\p{Z}', ''), ',', '.')::NUMERIC(11,3),
        REPLACE(REGEXP_REPLACE("Discount",  '\p{Z}', ''), ',', '.')::NUMERIC(6,3), 
        REPLACE(REGEXP_REPLACE("DiscountedPackagePrice",  '\p{Z}', ''), ',', '.')::NUMERIC(11,3),
        REGEXP_REPLACE("ShippedWithinTheSpecifiedPeriod",  '\p{Z}', '')::int,
        REGEXP_REPLACE("Completed",  '\p{Z}', '')::int,
        REPLACE(REGEXP_REPLACE("TheAmountOfRealtionInPurchasePrices",  '\p{Z}', ''), ',', '.')::NUMERIC(11,3),
        REPLACE(REGEXP_REPLACE("Revenue",  '\p{Z}', ''), ',', '.')::NUMERIC(11,3),
        "Invoice",
        "Course",
        TO_DATE("NumberOfRealization", 'DD:MM:YYYY'),
        "NumberRealization",
        "TheAmountOfRealPlacer"
FROM sttgaz.stage_erp_kit_sales AS s
LEFT JOIN sttgaz.dds_erp_counterparty AS c 
        ON s.CounterpartyID = c.CounterpartyID
LEFT JOIN sttgaz.dds_erp_сountry AS cnt
        ON HASH(s."Country", s."CountryKode") = HASH(cnt."Страна", cnt."Код страны")
WHERE DATE_TRUNC('month', TO_DATE("ShipmentMonth", 'DD:MM:YYYY'))::date IN (
        '{{execution_date.replace(day=1)}}',
        '{{(execution_date.replace(day=1) - params.delta).raplace(day=1)}}'
    )
