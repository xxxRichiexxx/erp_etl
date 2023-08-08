import requests
from pprint import pprint
import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote


params = {
    'stdate': 20230501,
    'enddate': 20230531,
    'csttp': 'fact',
}

response = requests.get(
    r'http://vs01uh/erp_demo/hs/sellers',
    auth=('get','123'),
    params=params,
)

response.encoding = 'utf-8-sig'

data = pd \
    .json_normalize(response.json()['test'])
pprint(data)

# updatedict = {
#         "Counterparty":sa.sql.sqltypes.VARCHAR(3000),
#         "CounterpartyID": sa.sql.sqltypes.VARCHAR(3000),
#         "Treaty": sa.sql.sqltypes.VARCHAR(3000),
#         "ApplicationNo": sa.sql.sqltypes.VARCHAR(3000),
#         "ApplicationContractingMonth": sa.sql.sqltypes.VARCHAR(3000),
#         "ShipmentMonth": sa.sql.sqltypes.VARCHAR(3000),
#         "Equipment": sa.sql.sqltypes.VARCHAR(3000),
#         "KitDrawingNumber": sa.sql.sqltypes.VARCHAR(3000),
#         "KitName": sa.sql.sqltypes.VARCHAR(3000),
#         "DrawingNumberPF": sa.sql.sqltypes.VARCHAR(3000),
#         "NumberOfKitsInTheApplication": sa.sql.sqltypes.VARCHAR(3000),
#         "Currency": sa.sql.sqltypes.VARCHAR(3000),
#         "KitPrice": sa.sql.sqltypes.VARCHAR(3000),
#         "Discount": sa.sql.sqltypes.VARCHAR(3000),
#         "DiscountedPackagePrice": sa.sql.sqltypes.VARCHAR(3000),
#         "ShippedWithinTheSpecifiedPeriod": sa.sql.sqltypes.VARCHAR(3000),
#         "Completed": sa.sql.sqltypes.VARCHAR(3000),
#         "TheAmountOfRealtionInPurchasePrices": sa.sql.sqltypes.VARCHAR(3000),
#         "Revenue": sa.sql.sqltypes.VARCHAR(3000),
#         "Invoice": sa.sql.sqltypes.VARCHAR(3000),
#         "PPSDate": sa.sql.sqltypes.VARCHAR(3000),
#         "Course": sa.sql.sqltypes.VARCHAR(3000),
#         "NumberOfRealization": sa.sql.sqltypes.VARCHAR(3000),
#         "NumberRealization": sa.sql.sqltypes.VARCHAR(3000),
#         "TheAmountOfRealPlacer": sa.sql.sqltypes.VARCHAR(3000),
# }

data.columns = [
        "Counterparty",
        "CounterpartyID",
        "Treaty",
        "TreatyID",
        "ApplicationNo",
        "ApplicationContractingMonth",
        "ShipmentMonth",
        "Equipment",
        "KitDrawingNumber",
        "KitName",
        "DrawingNumberPF",
        "NumberOfKitsInTheApplication",
        "Currency",
        "KitPrice",
        "Discount",
        "DiscountedPackagePrice",
        "ShippedWithinTheSpecifiedPeriod",
        "Completed",
        "TheAmountOfRealtionInPurchasePrices",
        "Revenue",
        "Invoice",
        "PPSDate",
        "Course",
        "NumberOfRealization",
        "NumberRealization",
        "TheAmountOfRealPlacer",
]

ps = quote('s@vy7hSA')
engine = sa.create_engine(
    f'vertica+vertica_python://shveynikovab:{ps}@vs-da-vertica:5433/sttgaz'
)


data.to_sql(
    'test2',
    engine,
    schema='sttgaz',
    if_exists='append',
    index=False,
)
