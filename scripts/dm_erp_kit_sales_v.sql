BEGIN TRANSACTION;

DROP VIEW IF EXISTS sttgaz.dm_erp_kit_sales_v;
CREATE OR REPLACE VIEW sttgaz.dm_erp_kit_sales_v AS
WITH 
	sq1 AS(
		SELECT
			Период 																	AS "Месяц",
			d.Наименование 															AS "Дивизион",
			c.Контрагент,
			s."Чертежный номер комплекта",
			s."Комплектация (вариант сборки)"										AS "Вариант сборки",
			s."Отгружено за указанный период",
			CASE
				WHEN cnt."Код страны" IS NULL AND c."Контрагент" = 'GAZ THANH DAT LIMITED LIABILITY COMPANY'
					THEN 'БЗ-Вьетнам'
				WHEN cnt."Код страны" IS NULL AND c."Контрагент" = 'АО АМЗ'
					THEN 'РФ-комплекты'
				WHEN cnt."Код страны" IS NULL AND c."Контрагент" = 'ООО Торговый дом "УГДК"'
					THEN 'СНГ-Украина' 
				WHEN cnt."Код страны" IS NULL AND c."Контрагент" = 'ГУ EMPRESA ESTATAL CUBANA IMPORTADORA Y EXPORTADORA DE PRODUCTOS'
					THEN 'БЗ-Куба' 
				WHEN cnt."Код страны" IN ('031', '051', '112', '398', '417', '498', '643', '762', '860' ) 
					THEN 'СНГ-' || INITCAP(cnt.Страна)
				ELSE 'БЗ-' || INITCAP(cnt.Страна)
			END 																	AS "Направление реализации с учетом УКП"
		FROM sttgaz.dds_erp_kit_sales 												AS s
		LEFT JOIN sttgaz.dds_erp_counterparty 										AS c 
			ON s."Контрагент ID" = c.id 
		LEFT JOIN sttgaz.dds_erp_сountry 											AS cnt 
			ON s."Страна ID"  = cnt.id
		LEFT JOIN sttgaz.dds_erp_division 											AS d 
			ON s."Дивизион ID" = d.id 
	),
	sq2 AS(
		SELECT
			"Месяц",
			"Дивизион",
			"Контрагент",
			"Чертежный номер комплекта",
			"Вариант сборки",
			"Направление реализации с учетом УКП",
			SUM("Отгружено за указанный период") AS "Реализовано"
		FROM sq1
		GROUP BY 
			"Месяц",
			"Дивизион",
			"Контрагент",
			"Чертежный номер комплекта",
			"Вариант сборки",
			"Направление реализации с учетом УКП"
	),
	sq3 AS(
		SELECT DISTINCT "Месяц"
		FROM sq2
	),
	sq4 AS(
		SELECT DISTINCT 
			"Дивизион",
			"Контрагент",
			"Чертежный номер комплекта",
			"Вариант сборки",
			"Направление реализации с учетом УКП"
		FROM sq2			
	),
	sq5 AS(
		SELECT *
		FROM sq3
		CROSS JOIN sq4
	),
	sq6 AS(
		SELECT
			sq5."Месяц",
			sq5."Дивизион",
			sq5."Контрагент",
			sq5."Чертежный номер комплекта",
			sq5."Вариант сборки",
			sq5."Направление реализации с учетом УКП",
			r1."Реализовано",
			r2."Реализовано"						AS "Реализовано АППГ"
		FROM sq5
		LEFT JOIN sq2 AS r1
			ON sq5."Месяц" = r1."Месяц"
			AND HASH(sq5."Дивизион", sq5."Контрагент",
					 sq5."Чертежный номер комплекта", sq5."Вариант сборки", sq5."Направление реализации с учетом УКП") =
				HASH(r1."Дивизион", r1."Контрагент",
					 r1."Чертежный номер комплекта", r1."Вариант сборки", r1."Направление реализации с учетом УКП")
		LEFT JOIN sq2 AS r2
			ON sq5."Месяц" = r1."Месяц" + INTERVAL '1 YEAR'
			AND HASH(sq5."Дивизион", sq5."Контрагент",
					 sq5."Чертежный номер комплекта", sq5."Вариант сборки", sq5."Направление реализации с учетом УКП") =
				HASH(r2."Дивизион", r2."Контрагент",
					 r2."Чертежный номер комплекта", r2."Вариант сборки", r2."Направление реализации с учетом УКП")
	)
SELECT
	*,
	SUM("Реализовано") OVER (
					PARTITION BY DATE_TRUNC('YEAR', "Месяц"), "Дивизион", "Контрагент",
					 "Чертежный номер комплекта", "Направление реализации с учетом УКП"
					ORDER BY "Месяц"
					) AS "Реализовано с начала года",
	SUM("Реализовано АППГ") OVER (
					PARTITION BY DATE_TRUNC('YEAR', "Месяц"), "Дивизион", "Контрагент",
					 "Чертежный номер комплекта", "Направление реализации с учетом УКП"
					ORDER BY "Месяц"
					) AS "Реализовано с начала прошлого года"				
FROM sq6
WHERE "Реализовано" IS NOT NULL
	OR "Реализовано АППГ" IS NOT NULL;

GRANT SELECT ON TABLE sttgaz.dm_erp_kit_sales_v TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_erp_kit_sales_v IS 'Реализация автокомплектов. Витрина данных с посчитанными метриками.';	

COMMIT TRANSACTION;