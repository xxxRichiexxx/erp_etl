import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import numpy as np


data = pd.read_excel(r"C:\Users\shveynikovab\Desktop\Факт реализации автокомплектов за 7 мес 2022.xlsx",
                     sheet_name = 'Лист1 (2)',
                     dtype_backend='pyarrow',
                     dtype="string[pyarrow]"
)
print(data)

data.columns = [
        "Counterparty",
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
]

print(data)


ps = quote('s@vy7hSA')
engine_2 = sa.create_engine(
    f'vertica+vertica_python://shveynikovab:{ps}@vs-da-vertica:5433/sttgaz'
)


data.to_sql(
    'stage_ADABAS_kit_sales',
    engine_2,
    schema = 'sttgaz',
    if_exists='append',
    index = False,
)