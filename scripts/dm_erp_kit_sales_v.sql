
DROP VIEW IF EXISTS sttgaz.dm_erp_kit_sales_v;
CREATE OR REPLACE VIEW sttgaz.dm_erp_kit_sales_v AS
WITH 
	sq AS(
		SELECT
			Период 																	AS "Месяц",
			d.Наименование 															AS "Дивизион",
			c.Контрагент,
			s."Чертежный номер комплекта",
			s."Отгружено за указанный период",
			CASE
				WHEN cnt."Код страны" IN ('031', '051', '112', '398', '417', '498', '643', '762', '860' ) THEN 'СНГ-' || cnt.Страна
				ELSE 'БЗ-' || cnt.Страна
			END 																	AS "Направление реализации с учетом УКП"
		FROM sttgaz.dds_erp_kit_sales 												AS s
		LEFT JOIN sttgaz.dds_erp_counterparty 										AS c 
			ON s."Контрагент ID" = c.id 
		LEFT JOIN sttgaz.dds_erp_сountry 											AS cnt 
			ON s."Страна ID"  = cnt.id
		LEFT JOIN sttgaz.dds_erp_division 											AS d 
			ON s."Дивизион ID" = d.id 
	)
SELECT
	"Месяц",
	"Дивизион",
	"Контрагент",
	"Чертежный номер комплекта",
	"Направление реализации с учетом УКП",
	SUM("Отгружено за указанный период") AS "Реализовано"
FROM sq
GROUP BY 
	"Месяц",
	"Дивизион",
	"Контрагент",
	"Чертежный номер комплекта",
	"Направление реализации с учетом УКП";