# ios 自然量 付费用户数，付费金额 占比 2022-10-01 ~ 2023-04-30

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getDataFromMC(sinceDayStr = '20230101', untilDayStr = '20230808'):
    sql = f'''
    '''

    print(sql)
    df = execSql(sql)
    return df