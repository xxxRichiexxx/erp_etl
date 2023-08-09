------------------------------stage------------------------------------------
DROP TABLE IF EXISTS sttgaz.stage_erp_kit_sales;
CREATE TABLE sttgaz.stage_erp_kit_sales(
        "Counterparty" varchar(1000),
        "CounterpartyID" varchar(1000),
        "Treaty" varchar(1000),
        "TreatyID" varchar(1000),
        "ApplicationNo" varchar(1000),
        "ApplicationContractingMonth" varchar(200),
        "ShipmentMonth" varchar(200),
        "Equipment" varchar(1000),
        "KitDrawingNumber" varchar(1000),
        "KitName" varchar(1000),
        "DrawingNumberPF" varchar(1000),
        "Division" varchar(200),
        "NumberOfKitsInTheApplication" varchar(1000),---------------------------------
        "Currency" varchar(100),
        "KitPrice" varchar(1000), -----------------------------------------------------
        "Discount" varchar(1000),------------------------------------------------- 
        "DiscountedPackagePrice" varchar(1000),--------------------------------------------
        "ShippedWithinTheSpecifiedPeriod" varchar(1000),----------------
        "Completed" varchar(1000),-------------------
        "TheAmountOfRealtionInPurchasePrices" varchar(1000),------------------------------------
        "Revenue" varchar(1000),---------------------------------
        "Invoice" varchar(1000),------------------------
        "Country" varchar(1000),
        "CountryKode" varchar(1000),
        "PPSDate" varchar(200),
        "Course" varchar(1000),
        "NumberOfRealization" varchar(200),
        "NumberRealization" varchar(1000),
        "TheAmountOfRealPlacer" varchar(1000),
        "load_date" date
)
ORDER BY ShipmentMonth
PARTITION BY DATE_TRUNC('month', load_date);



------------------------------DDS-----------------------------------------

DROP TABLE IF EXISTS sttgaz.dds_erp_counterparty;
CREATE TABLE sttgaz.dds_erp_counterparty (
	id AUTO_INCREMENT PRIMARY KEY,
	"CounterpartyID" varchar(1000),
	"Контрагент" varchar(1000),
        ts timestamp
);

DROP TABLE IF EXISTS sttgaz.dds_erp_сountry;
CREATE TABLE sttgaz.dds_erp_сountry (
	id AUTO_INCREMENT PRIMARY KEY,
        "Страна" varchar(1000),
        "Код страны" varchar(1000),
        ts timestamp
);


DROP TABLE IF EXISTS sttgaz.dds_erp_division;
CREATE TABLE sttgaz.dds_erp_division (
	id AUTO_INCREMENT PRIMARY KEY,
        "Наименование" varchar(1000),
        ts timestamp
);


DROP TABLE IF EXISTS sttgaz.dds_erp_kit_sales;
CREATE TABLE sttgaz.dds_erp_kit_sales(
	"Контрагент ID" INT REFERENCES sttgaz.dds_erp_counterparty(id),
        "Договор" varchar(1000),
        "Договор ID" varchar(1000),
        "Страна ID" INT REFERENCES sttgaz.dds_erp_сountry(id),
        "Номер приложения" varchar(1000),
        "Месяц контрактации" date,
        "Месяц отгрузки" date,
        "Комплектация (вариант сборки)" varchar(1000),
        "Чертежный номер комплекта" varchar(1000),
        "Наименование комплекта" varchar(1000),
        "Чертежный номер полуфабриката кабины" varchar(1000),
        "Дивизион ID" INT REFERENCES sttgaz.dds_erp_division(id),
        "Количество комплектов в приложении" int,
        "Валюта. Код" varchar(100),
        "Цена комплекта" numeric(11,3),
        "Скидка (процент)" numeric(6,3), 
        "Цена комплекта с учетом скидки" numeric(11,3),
        "Отгружено за указанный период" int,
        "Процент выполнения" numeric(11,3),
        "Сумма реал-ции в приходных ценах, руб." numeric(11,3),
        "Выручка" numeric(11,3),
        "Счет-фактура Номер" varchar(1000),
        "торг 12 дата" varchar(1000),
        "Валюта. Курс" varchar(1000),
        "Счет-фактура Дата" date,
        "Торг12 Номер" varchar(1000),
        "TheAmountOfRealPlacer" varchar(1000),
        "Период" date
)
ORDER BY "Месяц отгрузки"
PARTITION BY DATE_TRUNC('month', "Месяц отгрузки");

   	