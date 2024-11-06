import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json

def init():
    global execSql
    global dayStr


    if 'o' in globals():
        print('this is online version')

        from odps import options
        # UTC+0
        options.sql.settings = {
            'odps.sql.timezone':'Africa/Accra',
            "odps.sql.submit.mode" : "script"
        }

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
                return pd_df

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        dayStr = args['dayStr']
    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local
        dayStr = '20241104'  # 本地测试时的日期，可自行修改

def getHistoricalData(installDayStart,installDayEnd,platform='android'):
    app_package = 'com.fun.lastwar.gp' if platform == 'android' else 'id6448786147'

    sql = f'''
select
    app,
    install_day,
    media,
    country,
    group_name,
    pay_user_group_name,
    actual_pu,
    predicted_pu,
    actual_arppu,
    predicted_arppu,
    actual_revenue,
    predicted_revenue
from lastwar_predict_day1_pu_pct_by_cost_pct_verification
where day > 0
and install_day between {installDayStart} and {installDayEnd}
and app = '{app_package}'
;
    '''
    print(sql)
    data = execSql(sql)
    return data


def main():
    global dayStr

    # 统计往前推N天的数据
    N = 60

    # 找到上周的周一
    currentMonday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    lastMonday = currentMonday - pd.Timedelta(weeks=1)
    lastMondayStr = lastMonday.strftime('%Y%m%d')

    nDaysAgo = pd.to_datetime(lastMonday, format='%Y%m%d') - pd.Timedelta(days=N)
    nDaysAgoStr = nDaysAgo.strftime('%Y%m%d')

    # 获取历史数据
    historical_data = getHistoricalData(nDaysAgoStr,lastMondayStr)
    historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

    # # 1. 计算 天MAPE
    # dayDf = historical_data.groupby(['install_day', 'media', 'country', 'group_name']).agg({
    #     'actual_revenue': 'sum',
    #     'predicted_revenue': 'sum'
    # }).reset_index()

    # dayDf['mape_revenue'] = np.abs((dayDf['actual_revenue'] - dayDf['predicted_revenue']) / dayDf['actual_revenue']) * 100
    # dayDf2 = dayDf.groupby(['media', 'country','group_name']).agg({
    #     'mape_revenue': 'mean'
    # }).reset_index()
    
    # # 找到按照 media 和 country 分组的最小 MAPE 对应的 group_name，以及最小的 MAPE 值
    # minMapeDf = dayDf2.groupby(['media', 'country']).agg(
    #     minMape=('mape_revenue', 'min')
    # ).reset_index()
    # minMapeDf = minMapeDf.merge(dayDf2, on=['media', 'country'], how='left')
    # minMapeDf = minMapeDf[minMapeDf['mape_revenue'] == minMapeDf['minMape']]
    # print('按天的最小MAPE')
    # print(minMapeDf)

    # 2. 计算 周MAPE
    historical_data['week'] = historical_data['install_day'].dt.strftime('%Y-%U')
    weekDf = historical_data.groupby(['week', 'media', 'country', 'group_name']).agg({
        'actual_revenue': 'sum',
        'predicted_revenue': 'sum'
    }).reset_index()

    weekDf['mape_revenue'] = np.abs((weekDf['actual_revenue'] - weekDf['predicted_revenue']) / weekDf['actual_revenue']) * 100
    weekDf2 = weekDf.groupby(['media', 'country','group_name']).agg({
        'mape_revenue': 'mean'
    }).reset_index()

    # 找到按照 media 和 country 分组的最小 MAPE 对应的 group_name，以及最小的 MAPE 值
    minMapeDf2 = weekDf2.groupby(['media', 'country']).agg(
        minMape=('mape_revenue', 'min')
    ).reset_index()
    
    minMapeDf2 = minMapeDf2.merge(weekDf2, on=['media', 'country'], how='left')
    minMapeDf2 = minMapeDf2[minMapeDf2['mape_revenue'] == minMapeDf2['minMape']]
    # 只保留每个 media 和 country 组合的第一行
    minMapeDf2 = minMapeDf2.drop_duplicates(subset=['media', 'country'])

    print('按周的最小MAPE')
    print(minMapeDf2)

if __name__ == '__main__':
    init()
    main()