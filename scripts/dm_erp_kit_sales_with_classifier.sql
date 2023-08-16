SELECT
	Месяц,
	COALESCE(ks.Дивизион, n.Division)  		AS Дивизион,
	Контрагент,
	"Направление реализации с учетом УКП",
	Реализовано,
	Name 									AS Товар,
	Code65 									AS ТоварКод65,
	Manufacture								AS Производитель,
	property_value_name_1					AS "Классификатор подробно по дивизионам 22"
FROM sttgaz.dm_erp_kit_sales_v ks
LEFT JOIN sttgaz.stage_isc_nomenclature_guide n
ON ks."Чертежный номер комплекта" = REGEXP_REPLACE(n.ManufactureModel, '^А', 'A') 
	AND n.ManufactureModel <> ''
LEFT JOIN sttgaz.dm_isc_classifier_v c 
	ON n.Code65  = c.product_name 
	AND c.property_name = 'Подробный по дивизионам (с 2022 г)'