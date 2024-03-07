import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


import sys
sys.path.append('/src')
from src.maxCompute import execSql

apps = [
    {'name':'topwar','android':'com.topwar.gp','ios':'1479198816'},
    {'name':'lastwar','android':'com.fun.lastwar.gp','ios':'6448786147'},
    {'name':'topheroes','android':'com.greenmushroom.boomblitz.gp','ios':'6450953550'},
]

# 获得AF支付数据，按自然月汇总
def getAfPurchaseData(app, startDate='20230601', endDate='20240301', platform = 'ios'):
    filename = f'/src/data/afPurchase_{app}_{startDate}_{endDate}_{platform}.csv'
    print(filename)
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        appId = app[platform]
        if platform == 'ios':
            appId = 'id' + appId

        installStartDate = startDate[:4] + '-' + startDate[4:6] + '-' + startDate[6:8]

        sql = f'''
            select
                SUBSTRING(install_time, 1, 7) AS af_install_date,
                SUM(
                    case when event_timestamp - install_timestamp between 0 and 86400 
                        then event_revenue_usd else 0 
                    end
                ) as 24hours_revenue
            from ods_platform_appsflyer_events
            where 
                app_id = '{appId}'
                and event_name = 'af_purchase'
                and zone = 0
                and day between '{startDate}' and '{endDate}'
                and install_time >= '{installStartDate}'
            group by 
                af_install_date
            ;
        '''
        print(sql)
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data


def getBiPurchaseData(app, startDate='20230601', endDate='20240301', platform = 'ios'):
    filename = f'/src/data/biPurchase_{app["name"]}_{startDate}_{endDate}_{platform}.csv'
    print(filename)
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        appId = app[platform]
        if platform == 'ios':
            appId = 'id' + appId

        if app['name'] == 'topwar':
            sql = f'''
                select
                    to_char(from_unixtime(cast(install_timestamp as bigint)),'yyyy-mm') as af_install_date,
                    sum(
                        case when event_time - cast(install_timestamp as bigint) between 0 and 86400
                            then revenue_value_usd else 0
                        end
                    ) as 24hours_revenue
                from dwd_overseas_revenue_afattribution_realtime
                where
                    app_package = '{appId}'
                    and zone = 0
                    and day between '{startDate}' and '{endDate}'
                    and window_cycle = 9999
                    and install_day >= '{startDate}'
                group by af_install_date
                ;
            '''
        else:
            sql = f'''
                select
                    to_char(from_unixtime(cast(install_timestamp as bigint)),'yyyy-mm') as af_install_date,
                    sum(
                        case when event_time - cast(install_timestamp as bigint) between 0 and 86400
                            then revenue_value_usd else 0
                        end
                    ) as 24hours_revenue
                from dwd_overseas_revenue_allproject
                where
                    app_package = '{appId}'
                    and zone = 0
                    and day between '{startDate}' and '{endDate}'
                    and install_day >= '{startDate}'
                group by af_install_date
                ;
            '''

        print(sql)
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def main():
    startDate = '20230601'
    endDate = '20240301'
    for app in apps:
        afDataDf = getAfPurchaseData(app, startDate, endDate)
        biDataDf = getBiPurchaseData(app, startDate, endDate)

        df = pd.merge(afDataDf, biDataDf, how='outer', on='af_install_date', suffixes=('_af', '_bi'))
        df = df[df['af_install_date'] < '2024-03']
        df = df.sort_values(by='af_install_date', ascending=True)

        # 计算差异
        df['diff'] = (df['24hours_revenue_af'] - df['24hours_revenue_bi']) / df['24hours_revenue_bi']
        print(df)

        # 画图
        fig, ax = plt.subplots(figsize=(16, 6))
        
        # 设置af_install_date为x轴
        x = df['af_install_date']

        # 设置第一个Y轴显示af数据和bi数据
        ax.plot(x, df['24hours_revenue_af'], label='AF Revenue')
        ax.plot(x, df['24hours_revenue_bi'], label='BI Revenue')

        # 创建第二个Y轴，显示diff数据
        ax2 = ax.twinx()
        ax2.plot(x, df['diff'], label='Difference', linestyle='--')

        # 设置标题和标签
        ax.set_title('AF vs BI Revenue')
        ax.set_xlabel('Install Date')
        ax.set_ylabel('Revenue')
        ax2.set_ylabel('Difference')

        # 显示图例
        ax.legend(loc='upper left')
        ax2.legend(loc='upper right')

        plt.savefig(f'/src/data/afVsBi_{app["name"]}.png')

if __name__ == '__main__':
    main()
    