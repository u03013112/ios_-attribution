# 将预测结果，与真实数据做比对
# 要计算所有的MAPE，R2，相关性系数
# 还要按月进行上述指标的计算
# 然后将结果写成Df和一个主要指标
# 为了后面做模型的评价
# 后面可以再画图

import pandas as pd
import sys
sys.path.append('/src')

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

# 要求df包含install_date,revenue_7d
def check(retDf):
    df0 = pd.read_csv(getFilename('20230919_analyze3'),dtype={'install_date':str})
    df0GroupbyInstallDate = df0.groupby('install_date').sum()
    df0GroupbyInstallDate = df0GroupbyInstallDate[['install_date','revenue_7d']]

    df = pd.merge(retDf,df0GroupbyInstallDate,on='install_date',how='left',suffixes=('_pred','_real'))
    df['mape'] = (df['revenue_7d_pred'] - df['revenue_7d_real']).abs() / df['revenue_7d_real']

    return df['mape'].mean()


