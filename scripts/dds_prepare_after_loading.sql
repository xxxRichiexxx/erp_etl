INSERT INTO sttgaz.dds_erp_kit_sales 
	("Контрагент ID", ---
	 "Договор",----
	 "Договор ID",----
	 "Страна ID",----
	 "Номер приложения",----
	 "Месяц контрактации",----
	 "Месяц отгрузки",---
	 "Комплектация (вариант сборки)",---
	 "Чертежный номер комплекта",----
	 "Наименование комплекта",----
	 "Чертежный номер полуфабриката кабины",-----
	 "Дивизион ID",----
	 "Количество комплектов в приложении",-----
	 "Валюта. Код",-----
	 "Цена комплекта",-----
	 "Скидка (процент)",-----
	 "Цена комплекта с учетом скидки",-------
	 "Отгружено за указанный период",------
	 "Процент выполнения",-------
	 "Сумма реал-ции в приходных ценах, руб.",-----
	 "Выручка",------
	 "Счет-фактура Номер",-------
	 "торг 12 дата",
	 "Валюта. Курс",-------
	 "Счет-фактура Дата",
	 "Торг12 Номер",
	 "TheAmountOfRealPlacer",
	 "Период")
WITH 
	Counterparty_Country AS (
		SELECT DISTINCT 
			Counterparty,
			Country
		FROM sttgaz.stage_erp_kit_sales seks 
	),
	data_from_adabas AS(
		SELECT
			CASE 
				WHEN "Counterparty" = 'ТОО "СемАЗ", БИН-060240015888' THEN 'СемАЗ ТОО, БИН-060240015888'
				WHEN "Counterparty" = 'ТОО "Daewoo Bus Kazakhstan", БИН-070140000967' THEN 'Daewoo Bus Kazakhstan, БИН-070140000967 ТОО'
				WHEN "Counterparty" = 'ООО "АЗЕРМАШ ПАРК"' THEN 'АЗЕРМАШ ПАРК ООО'
				ELSE '??????'
			END	AS "Counterparty",
		        "Treaty",
		        "ApplicationNo",
		        "ShipmentMonth",
		        "KitNo",
		        "Equipment",
		        "KitDrawingNumber",
		        "DrawingNumberPF",
		        "Color",
		        "Quantity",
		        "Currency",
		        "Course",
		        "KitPrice", 
		        "Discount",
		        "DiscountedPackagePrice",
		        "PFCabinsQuantity",
		        "AmountOfRealtionPFCabins", 
		        "AmountOfRealtionPlacer",
		        "Torg12No",
		        "Torg12Date",
		        "InvoiceNo",
		        "InvoiceDate",
		        "Specification",
		        "TheAmountOfRealtionInPurchasePrices",
		        "PPSDate"	
		FROM sttgaz.stage_ADABAS_kit_sales
		WHERE KitDrawingNumber IS NOT NULL
	),
source_data AS(
	SELECT *
	FROM data_from_adabas
	LEFT JOIN Counterparty_Country USING (Counterparty)
)
SELECT
	c.id  															AS "Контрагент ID",---
	Treaty															AS "Договор",----
	NULL 															AS "Договор ID",----
	cnt.id 															AS "Страна ID",----
	ApplicationNo													AS "Номер приложения",----
	NULL 															AS "Месяц контрактации",----
	TO_DATE(ShipmentMonth, 'MM.YYYY')								AS "Месяц отгрузки", ------------------------Ispravit
	Equipment  														AS "Комплектация (вариант сборки)",---
	REPLACE(DrawingNumberPF, ' ', '')								AS "Чертежный номер комплекта",----
	REGEXP_REPLACE(KitDrawingNumber, '^[A-ZА-Я0-9]{6}-\d{7} ', '')	AS "Наименование комплекта",-----
	REPLACE(DrawingNumberPF, ' ', '')								AS "Чертежный номер полуфабриката кабины",-----
	NULL 															AS "Дивизион ID",-----
	Quantity														AS "Количество комплектов в приложении",-----
	Currency														AS "Валюта. Код",----
	KitPrice														AS "Цена комплекта",-----
	Discount														AS "Скидка (процент)",--------
	DiscountedPackagePrice 											AS "Цена комплекта с учетом скидки",----
	PFCabinsQuantity												AS "Отгружено за указанный период",-------
	ROUND(100*PFCabinsQuantity/Quantity, 0)							AS "Процент выполнения",------
	TheAmountOfRealtionInPurchasePrices								AS "Сумма реал-ции в приходных ценах, руб.",-----
	AmountOfRealtionPFCabins + AmountOfRealtionPlacer 
		- TheAmountOfRealtionInPurchasePrices						AS "Выручка",--------
	InvoiceNo														AS "Счет-фактура Номер",--------
	TO_DATE(Torg12Date, 'DD.MM.YYYY')								AS "торг 12 дата",
	Course															AS "Валюта. Курс",---------
	TO_DATE(InvoiceDate, 'DD.MM.YYYY')								AS "Счет-фактура Дата",
	Torg12No														AS "Торг12 Номер",
	AmountOfRealtionPlacer											AS "TheAmountOfRealPlacer",
	TO_DATE(ShipmentMonth, 'MM.YYYY')								AS "Период" -----------------------------Ispravit
FROM source_data AS d
LEFT JOIN sttgaz.dds_erp_counterparty AS c 
	ON d.Counterparty = c.Контрагент 
LEFT JOIN sttgaz.dds_erp_сountry AS cnt 
	ON d.Country =  cnt.Страна 

