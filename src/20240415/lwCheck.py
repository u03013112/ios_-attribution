# 对比lastwar 24小时付费差异
# 获取af中af_sdk_update_skan所有订单
# 获取bi中的24小时内所有订单
# 结合进行对比

import os
import sys
import pandas as pd

sys.path.append('/src')
from src.maxCompute import execSql


def getAfData():
    filename = '/src/data/20240415afSdkUpdateSkan.csv'
    if not os.path.exists(filename):
        sql = '''
select
customer_user_id,
install_time,
event_time,
get_json_object(base64decode(event_value), '$.af_order_id') as order_id,
event_revenue_usd
from
ods_platform_appsflyer_events
where
zone = 0
and app = 502
and app_id = 'id6448786147'
and day between '20240403'
and '20240408'
and event_name = 'af_sdk_update_skan';
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:', filename)
        df = pd.read_csv(filename)

    return df

def getBiData():
    filename = '/src/data/20240415bi24Hours.csv'
    if not os.path.exists(filename):
        sql = '''
select
  game_uid,
  order_id,
  install_timestamp,
  install_day,
  event_time,
  revenue_value_usd
from
  dwd_lastwar_order_revenue
where
  app_package = 'id6448786147'
  and (event_time - install_timestamp) between 0
  and 86400;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:', filename)
        df = pd.read_csv(filename)

    return df

def main():
    dfAf = getAfData()
    dfBi = getBiData()

    # 数据简单处理
    # 将order_id转为str
    dfAf['order_id'] = dfAf['order_id'].astype(str)
    dfBi['order_id'] = dfBi['order_id'].astype(str)

    # 按照event_time,转成event_day
    dfAf['event_time'] = pd.to_datetime(dfAf['event_time'])
    dfAf['event_day'] = dfAf['event_time'].dt.strftime('%Y%m%d')
    # bi表中的event_time是unix时间戳，需要转换
    dfBi['event_time'] = pd.to_datetime(dfBi['event_time'], unit='s')
    dfBi['event_day'] = dfBi['event_time'].dt.strftime('%Y%m%d')
    # af表列event_revenue_usd改名 revenue_usd
    dfAf.rename(columns={'event_revenue_usd':'revenue_usd'}, inplace=True)
    # bi表列revenue_value_usd改名 revenue_usd
    dfBi.rename(columns={'revenue_value_usd':'revenue_usd'}, inplace=True)

    dfAfGroupby = dfAf.groupby('event_day').agg({'order_id':'count', 'revenue_usd':'sum'}).reset_index()
    dfBiGroupby = dfBi.groupby('event_day').agg({'order_id':'count', 'revenue_usd':'sum'}).reset_index()

    df = pd.merge(dfAfGroupby, dfBiGroupby, how='outer', on=['event_day'], suffixes=('_af', '_bi')).reindex()
    df = df[(df['event_day'] >= '20240403') & (df['event_day'] <= '20240408')]

    df['usd diff'] = (df['revenue_usd_af'] - df['revenue_usd_bi'])/df['revenue_usd_bi']
    df['order_id diff'] = (df['order_id_af'] - df['order_id_bi'])/df['order_id_bi']

    # usd diff 和 order_id diff 改为 百分比，保留两位小数
    df['usd diff'] = df['usd diff'].apply(lambda x: format(x, '.2%'))
    df['order_id diff'] = df['order_id diff'].apply(lambda x: format(x, '.2%'))

    df.to_csv('/src/data/20240415diff.csv', index=False)

# af比bi高的比较容易理解
# 大量出现在没有重启，开关一直保持打开大致的。主要是这个比例比较正常。
def afMoreThanBi():
    dfAf = getAfData()
    dfBi = getBiData()

    # 数据简单处理
    # 将order_id转为str
    dfAf['order_id'] = dfAf['order_id'].astype(str)
    dfBi['order_id'] = dfBi['order_id'].astype(str)

    # 按照event_time,转成event_day
    dfAf['event_time'] = pd.to_datetime(dfAf['event_time'])
    # bi表中的event_time是unix时间戳，需要转换
    dfBi['event_time'] = pd.to_datetime(dfBi['event_time'], unit='s')

    # af表列event_revenue_usd改名 revenue_usd
    dfAf.rename(columns={'event_revenue_usd':'revenue_usd'}, inplace=True)
    # bi表列revenue_value_usd改名 revenue_usd
    dfBi.rename(columns={'revenue_value_usd':'revenue_usd'}, inplace=True)

    dfAf = dfAf[['order_id', 'event_time', 'revenue_usd','customer_user_id','install_time']]
    dfBi = dfBi[['order_id', 'event_time', 'revenue_usd']]
    
    df = pd.merge(dfAf, dfBi, how='outer', on=['order_id'], suffixes=('_af', '_bi')).reindex()
    df = df.fillna(0)
    afMoreThanBiDf = df[df['revenue_usd_bi'] == 0]
    print(afMoreThanBiDf)


# af比bi低的需要在调查一下
def afLessThanBi():
    dfAf = getAfData()
    dfBi = getBiData()

    # 数据简单处理
    # 将order_id转为str
    dfAf['order_id'] = dfAf['order_id'].astype(str)
    dfBi['order_id'] = dfBi['order_id'].astype(str)

    # 按照event_time,转成event_day
    dfAf['event_time'] = pd.to_datetime(dfAf['event_time'])
    dfAf['event_day'] = dfAf['event_time'].dt.strftime('%Y%m%d')
    # bi表中的event_time是unix时间戳，需要转换
    dfBi['event_time'] = pd.to_datetime(dfBi['event_time'], unit='s')
    dfBi['event_day'] = dfBi['event_time'].dt.strftime('%Y%m%d')

    # af表列event_revenue_usd改名 revenue_usd
    dfAf.rename(columns={'event_revenue_usd':'revenue_usd'}, inplace=True)
    # bi表列revenue_value_usd改名 revenue_usd
    dfBi.rename(columns={'revenue_value_usd':'revenue_usd'}, inplace=True)

    dfAf = dfAf[['order_id', 'event_time', 'revenue_usd','customer_user_id','install_time']]
    dfBi = dfBi[['order_id', 'event_time','event_day','revenue_usd','install_day']]
    dfBi = dfBi[(dfBi['event_day'] >= '20240403') & (dfBi['event_day'] <= '20240408')]

    df = pd.merge(dfAf, dfBi, how='outer', on=['order_id'], suffixes=('_af', '_bi')).reindex()
    df = df.fillna(0)
    
    afLessThanBiDf = df[df['revenue_usd_af'] == 0]
    print(afLessThanBiDf) 

    
if __name__ == '__main__':
    # main()
    # afMoreThanBi()
    afLessThanBi()