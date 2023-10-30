import os
import pandas as pd
import numpy as np

import sys
sys.path.append('/src')
from src.maxCompute import execSqlBj as execSql

def getData():
    filename = '/src/data/zk2/20231027data1.csv'
    if os.path.exists(filename):
        return pd.read_csv(filename)
    sql = '''
        select 
            revenue_value_cny
        from 
            dwd_wx_revenue_afattribution
        where
            mediasource = 'touTiaoV2'
            and day > 20230901
            and purchase_day = install_day
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv('/src/data/zk2/20231027data1.csv',index=False)
    return df

# 返回使用新的步长的付费次数的总数 比 原本的付费次数 的 比例
def getNewPayCountRate(df,col='revenue_1d',stepValue=6.0):
    dfCopy = df.copy()
    dfCopy['payCount'] = 1
    dfCopy['newPayCount'] = np.ceil(dfCopy[col]/stepValue)
    return dfCopy['newPayCount'].sum()/dfCopy['payCount'].sum()

def main():
    # df = getData()
    df = pd.read_csv('/src/data/zk2/20231027data1_20230901.csv')

    # 对df的列revenue_value_cny进行汇总，计算每一种revenue_value_cny的count
    df2 = df.groupby('revenue_value_cny').size().reset_index(name='count')
    print(df2)
    df2.to_csv('/src/data/zk2/20231027data2.csv')

    stepValueList = []
    newPayCountRateList = []
    for stepValue in np.arange(6,13):
        newPayCountRate = getNewPayCountRate(df,col = 'revenue_value_cny',stepValue=stepValue)
        stepValueList.append(stepValue)
        newPayCountRateList.append(newPayCountRate)

    retDf = pd.DataFrame({
        'stepValue':stepValueList,
        'newPayCountRate':newPayCountRateList
    })
    
    retDf.to_csv('/src/data/zk2/20231027Ret.csv',index=False)
    print(retDf)

if __name__ == '__main__':
    main()