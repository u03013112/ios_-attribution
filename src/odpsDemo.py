from odps import ODPS
from config import accessId,secretAccessKey,defaultProject,endPoint
# print(accessId,secretAccessKey,defaultProject,endPoint)

def execSql():
    o = ODPS(accessId, secretAccessKey, defaultProject,
                endpoint=endPoint)
    sql='''
        select
        *
        from ods_platform_appsflyer_skad_details
        where
        skad_conversion_value=63
        and day='20220901'
        limit 1000;
    '''
    with o.execute_sql(sql).open_reader() as reader:
        # for record in reader:
        #     print(record)
        pd_df = reader.to_pandas()
        # print(pd_df)
        pd_df.to_csv('/src/data/a.csv')

import pandas as pd
def readCsv():
    pd_df = pd.read_csv('/src/data/a.csv')
    print(pd_df)

if __name__ == '__main__':
    readCsv()