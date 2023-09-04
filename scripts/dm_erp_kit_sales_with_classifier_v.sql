BEGIN TRANSACTION;

DROP VIEW IF EXISTS sttgaz.dm_erp_kit_sales_with_classifier_v;
CREATE OR REPLACE VIEW sttgaz.dm_erp_kit_sales_with_classifier_v AS
	SELECT
		Месяц,
		"Направление реализации с учетом УКП",
		COALESCE(s.Дивизион, n.Дивизион)  		AS Дивизион,
		Производитель							AS Производитель,
		property_value_name_1					AS "Классификатор подробно по дивизионам 22",
		Наименование 							AS Товар,
		Код65 									AS ТоварКод65,
		"Реализовано",
		NULL::NUMERIC(12,2) 					AS "Оборот",
		NULL::NUMERIC(12,2) 					AS "Оборот без НДС",
		NULL::NUMERIC(12,2) 					AS "Сумма возмещения без НДС",
		"Реализовано АППГ",
		NULL::NUMERIC(12,2)						AS "Оборот АППГ",
		NULL::NUMERIC(12,2) 					AS "Оборот без НДС АППГ",
		NULL::NUMERIC(12,2) 					AS "Сумма возмещения без НДС АППГ",
		"Реализовано с начала года",
		"Реализовано с начала прошлого года",
		CASE
			WHEN "Направление реализации с учетом УКП" LIKE 'РФ-%'
				THEN 'РФ'
			WHEN "Направление реализации с учетом УКП" LIKE 'СНГ-%'
				THEN 'СНГ'
			WHEN "Направление реализации с учетом УКП" LIKE 'ДРКП -%'
				THEN 'ДРКП'
			WHEN "Направление реализации с учетом УКП" IS NULL
				THEN NULL
			ELSE
				'Прочее'
			END AS "Направление",
		CASE
			WHEN "Производитель" LIKE '%ГАЗ%'
				THEN 'ГАЗ'
			WHEN "Производитель" LIKE '%ПАЗ%'
				THEN 'ПАЗ'
			WHEN "Производитель" LIKE '%КАВЗ%'
				THEN 'КАВЗ'
			ELSE
				'Прочее'
			END AS "Завод"
	FROM sttgaz.dm_erp_kit_sales_v s
	LEFT JOIN sttgaz.dds_isc_nomenclature_guide n
		ON (s."Чертежный номер комплекта" = n."Модель на заводе"
				OR REPLACE(s."Чертежный номер комплекта", '-00', '-')= REGEXP_REPLACE(REGEXP_REPLACE(n.Код65, '^А', 'A'), '^С', 'C')
			AND n."Модель на заводе" <> '')
			AND UPPER(REPLACE(n."Производитель", ' ', '')) = 'ГАЗПАО'
			AND n."Наименование" <>'Комплект автомобил'
	LEFT JOIN sttgaz.dm_isc_classifier_v c 
		ON REGEXP_REPLACE(n.Код65, '^А', 'A') = REGEXP_REPLACE(c.product_name, '^А', 'A') 
			AND c.property_name = 'Подробный по дивизионам (с 2022 г)';

GRANT SELECT ON TABLE sttgaz.dm_erp_kit_sales_with_classifier_v TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_erp_kit_sales_with_classifier_v IS 'Реализация автокомплектов. Витрина данных с посчитанными метриками и классификатором.';	

COMMIT TRANSACTION;