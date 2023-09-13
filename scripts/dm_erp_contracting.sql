SELECT DROP_PARTITIONS(
    'sttgaz.dm_erp_contracting',
    '{execution_date}',
    '{execution_date}'
);

INSERT INTO sttgaz.dm_erp_contracting 
WITH 
	base_query AS(
		 SELECT
			"Месяц контрактации",
			"Месяц отгрузки",
			NULL																								AS "Дилер",
			'ГАЗ ПАО ' 																							AS "Производитель",
			NULL                                                    											AS "Город",
			'Неизвестно'                                                   										AS "Вид оплаты",
			'Комплекты'                             															AS "Вид продукции",
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
			END 																								AS "Направление реализации",
			"Отгружено за указанный период"																		AS "Количество",
			HASH("Направление реализации", "Дилер", "Производитель", "Город", "Вид оплаты", "Вид продукции") 	AS key 	
		FROM sttgaz.dds_erp_kit_sales 																			AS s
		LEFT JOIN sttgaz.dds_erp_counterparty 																	AS c 
			ON s."Контрагент ID" = c.id 
		LEFT JOIN sttgaz.dds_erp_сountry 																		AS cnt 
			ON s."Страна ID"  = cnt.id
		LEFT JOIN sttgaz.dds_erp_division 																		AS d 
			ON s."Дивизион ID" = d.id 	
        WHERE s."Месяц отгрузки" > DATE_TRUNC('month', '{execution_date}'::date - INTERVAL '8 month')::date

	),
	matrix AS(
		SELECT DISTINCT 
			'{execution_date}'::date								AS "Период",
			"Направление реализации",
			"Дилер",
			"Производитель",    
			"Город",
			"Вид оплаты",
			"Вид продукции",
			key                       -----?
		FROM base_query
	),
	sq1 AS(
		SELECT
			key,
			SUM(Количество) 									AS "Догруз на начало месяца"
		FROM base_query
		WHERE "Месяц контрактации" >= DATE_TRUNC('MONTH', '{execution_date}'::date)   --- "Месяц отгрузки"
			AND "Месяц отгрузки"  < DATE_TRUNC('MONTH', '{execution_date}'::date)   ----- "Месяц контрактации"
		GROUP BY key		
	),
	sq2 AS(
		 SELECT
			key,
		 	SUM(Количество) 									AS "План контрактации",
		 	ROUND(SUM(Количество)*0.7, 0) 						AS "План контрактации. Неделя 1",
		 	ROUND(SUM(Количество)*0.2, 0) 						AS "План контрактации. Неделя 2",
		 	ROUND(SUM(Количество)*0.05, 0) 						AS "План контрактации. Неделя 3",
		 	SUM(Количество) - ROUND(SUM(Количество)*0.7, 0) 
		 					- ROUND(SUM(Количество)*0.05, 0)
		 					- ROUND(SUM(Количество)*0.2, 0)		AS "План контрактации. Неделя 4"
		FROM base_query
		WHERE DATE_TRUNC('MONTH', "Месяц отгрузки") = DATE_TRUNC('MONTH', '{execution_date}') ----execution_date - priveden k nf4alu mesiatsa
		GROUP BY key	
	),
	sq3 AS(
		SELECT
		    key,
		 	SUM(Количество) 									AS "Факт выдачи ОР"
		FROM base_query
		WHERE DATE_TRUNC('MONTH', "Месяц отгрузки") = DATE_TRUNC('MONTH', '{execution_date}') ----execution_date - priveden k nf4alu mesiatsa
		GROUP BY key
	),
	sq4 AS(
		 SELECT 
			key,
		 	SUM(Количество) 									AS "Догруз на конец месяца"
		 FROM base_query
		 WHERE DATE_TRUNC('MONTH', "Месяц отгрузки") <= DATE_TRUNC('MONTH', '{execution_date}')
			AND DATE_TRUNC('MONTH', "Месяц контрактации") > DATE_TRUNC('MONTH', '{execution_date}')
		 GROUP BY key
	),
	sq5 AS(
		 SELECT
			key,
		 	SUM(Количество) 									AS "Отгрузка в счет следующего месяца" 
		 FROM base_query
		 WHERE DATE_TRUNC('MONTH', "Месяц отгрузки") = '{next_month}' 
		 	AND DATE_TRUNC('MONTH', "Месяц контрактации") = DATE_TRUNC('MONTH', '{execution_date}')
		GROUP BY key
	),
	sq6 AS(
		 SELECT
			key,
		 	SUM(Количество) 									AS "Отгрузка в предыдущем месяце из плана текущего месяца" 
		 FROM base_query
		 WHERE DATE_TRUNC('MONTH', "Месяц отгрузки")::date = DATE_TRUNC('MONTH', '{execution_date}')::date
		 	AND DATE_TRUNC('MONTH', "Месяц контрактации")::date = '{previous_month}' 
		GROUP BY key
	),
	sq7 AS(
		SELECT
			"Дата",
			ts,
			"Направление реализации",
			NULL 																								AS "Дилер",
			"Производитель",
			NULL 																								AS "Город",
			"Вид оплаты",
			"Вид продукции",
			HASH("Направление реализации", "Дилер", "Производитель", "Город", "Вид оплаты", "Вид продукции") 	AS key,
			SUM("План контрактации")																			AS "План контрактации",
			SUM("План контрактации. Неделя 1")																	AS "План контрактации. Неделя 1",
			SUM("План контрактации. Неделя 2")																	AS "План контрактации. Неделя 2",
			SUM("План контрактации. Неделя 3")																	AS "План контрактации. Неделя 3",
			SUM("План контрактации. Неделя 4")																	AS "План контрактации. Неделя 4"
		FROM sttgaz.dm_isc_contracting_plan
		WHERE DATE_TRUNC('minute', ts) = (
				SELECT DATE_TRUNC('minute', MIN(ts))
				FROM sttgaz.dm_isc_contracting_plan
				WHERE "Дата" = '{plan_date}'
			)
			AND "Дата" = '{plan_date}'
			AND "Вид продукции" = 'Комплекты'
		GROUP BY "Дата",
				 ts,
				 "Направление реализации",
				 "Дилер",
				 "Производитель",
				 "Город",
				 "Вид оплаты",
				 "Вид продукции",
				 key
	)
SELECT
	COALESCE(m."Период", DATE_TRUNC('MONTH', sq7.Дата))					AS "Период",
	COALESCE(m."Направление реализации", sq7."Направление реализации") 	AS "Направление реализации",
	COALESCE(m."Дилер", sq7."Дилер") 									AS "Дилер",
	COALESCE(m."Производитель", sq7."Производитель")					AS "Производитель",
	COALESCE(m."Город", sq7."Город")									AS "Город",
	COALESCE(m."Вид оплаты", sq7."Вид оплаты") 							AS "Вид оплаты",
	COALESCE(m."Вид продукции", sq7."Вид продукции")					AS "Вид продукции",
	"Догруз на начало месяца",
	sq2."План контрактации",
	sq2."План контрактации. Неделя 1",
	sq2."План контрактации. Неделя 2",
	sq2."План контрактации. Неделя 3",
	sq2."План контрактации. Неделя 4",
	"Факт выдачи ОР",
	"Догруз на конец месяца",
	"Отгрузка в счет следующего месяца",
	"Отгрузка в предыдущем месяце из плана текущего месяца",
	sq7."План контрактации",
	sq7."План контрактации. Неделя 1",
	sq7."План контрактации. Неделя 2",
	sq7."План контрактации. Неделя 3",
	sq7."План контрактации. Неделя 4",
	sq7."ts"::date
FROM matrix AS m
LEFT JOIN sq1
	ON m.key = sq1.key
LEFT JOIN sq2
	ON m.key = sq2.key
LEFT JOIN sq3
	ON m.key = sq3.key
LEFT JOIN sq4
	ON m.key = sq4.key
LEFT JOIN sq5
	ON m.key = sq5.key
LEFT JOIN sq6
	ON m.key = sq6.key
FULL JOIN sq7
	ON m.key = sq7.key
WHERE
	"Догруз на начало месяца" IS NOT NULL
	OR sq2."План контрактации" IS NOT NULL
	OR sq2."План контрактации. Неделя 1" IS NOT NULL
	OR sq2."План контрактации. Неделя 2" IS NOT NULL
	OR sq2."План контрактации. Неделя 3" IS NOT NULL
	OR sq2."План контрактации. Неделя 4" IS NOT NULL
	OR "Факт выдачи ОР" IS NOT NULL
	OR "Догруз на конец месяца" IS NOT NULL
	OR "Отгрузка в счет следующего месяца" IS NOT NULL
	OR "Отгрузка в предыдущем месяце из плана текущего месяца" IS NOT NULL
	OR sq7."План контрактации" IS NOT NULL
	OR sq7."План контрактации. Неделя 1" IS NOT NULL
	OR sq7."План контрактации. Неделя 2" IS NOT NULL
	OR sq7."План контрактации. Неделя 3" IS NOT NULL
	OR sq7."План контрактации. Неделя 4" IS NOT NULL;