import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql


# 找到lastwar iOS和Android的广告起量时间
# 获得广告信息，花费与展示量
# 顺便计算以下CPM变动
# 然后画图
# 预计是在11-10日左右起量，所以从11-01开始一直到现在的数据
def getAdData():
    sql = '''
        select
            install_day,
            app_package,
            country,
            sum(impression) as impression,
            sum(cost_value_usd) as cost
        from
            dwd_overseas_cost_allproject
        WHERE
            app = '502'
            and mediasource = 'googleadwords_int'
        group by
            install_day,
            country,
            app_package
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv('/src/data/lastwarAdData.csv',index=False)
    return df

def step1():
    # getAdData()
    df = pd.read_csv('/src/data/lastwarAdData.csv')
    df['cpm'] = df['cost']*1000/df['impression']
    # install_day是类似20231101的字符串，需要转换成datetime
    df['install_day'] = pd.to_datetime(df['install_day'],format='%Y%m%d')

    appPackageList = df['app_package'].unique().tolist()
    geoList = ['KR','JP','US']
    for appPackage in appPackageList:
        for geo in geoList:
            appDf = df.loc[
                (df['app_package']==appPackage) &
                (df['country']==geo)
            ].copy()
            date_range = pd.date_range(start=appDf['install_day'].min(), end=appDf['install_day'].max())
            date_df = pd.DataFrame(date_range, columns=['install_day'])
            appDf = pd.merge(date_df, appDf, on='install_day', how='left')
            appDf = appDf.fillna(0)
            appDf = appDf.sort_values(by='install_day').reset_index(drop=True)
            
            # 画图，3张横着的图，纵向合并在一张图里
            # 3张图合在一起，横坐标是install_day，纵坐标是impression、cost、cpm
            # 保存图片 ‘/src/data/lastwarAdData.jpg’
            # 创建子图
            fig, axs = plt.subplots(3, 1, figsize=(10, 15))

            # 画图，3张横着的图，纵向合并在一张图里
            # 3张图合在一起，横坐标是install_day，纵坐标是impression、cost、cpm
            axs[0].plot(appDf['install_day'], appDf['impression'])
            axs[0].set_title('Impression over time')
            axs[0].set_ylabel('Impression')

            axs[1].plot(appDf['install_day'], appDf['cost'])
            axs[1].set_title('Cost over time')
            axs[1].set_ylabel('Cost')

            axs[2].plot(appDf['install_day'], appDf['cpm'])
            axs[2].set_title('CPM over time')
            axs[2].set_ylabel('CPM')

            # 自动调整子图间距
            plt.tight_layout()

            # 保存图片
            plt.savefig(f'/src/data/lastwarAdData_{appPackage}_{geo}.jpg')
            plt.close()

# 获取topwar对应时间段的广告数据
# 环比，同比
# 画图
def step2():
    # # 直接获取2023-09-22日到今天的topwar广告数据
    # sql = '''
    #     SELECT
    #         install_day,
    #         app_package,
    #         country,
    #         sum(impression) as impression,
    #         sum(cost_value_usd) as cost
    #     FROM
    #         rg_bi.dwd_overseas_cost_new
    #     WHERE
    #         app = '102'
    #         AND zone = '0'
    #         AND cost_value_usd > 0
    #         AND window_cycle = 9999
    #         AND mediasource = 'googleadwords_int'
    #         and install_day between 20230922
    #         and 20231122
    #     group by
    #         install_day,
    #         country,
    #         app_package
    #     ;
    # '''
    # print(sql)
    # df = execSql(sql)
    # df.to_csv('/src/data/topwarAdData.csv',index=False)
    # # 再获得2022-09-22日到2022-11-22日的topwar广告数据
    # sql2 = '''
    #     SELECT
    #         install_day,
    #         app_package,
    #         sum(impression) as impression,
    #         sum(cost_value_usd) as cost
    #     FROM
    #         rg_bi.dwd_overseas_cost_history
    #     WHERE
    #         app = '102'
    #         AND zone = '0'
    #         AND cost_value_usd > 0
    #         AND mediasource = 'googleadwords_int'
    #         and install_day between 20220922
    #         and 20221122
    #     group by
    #         install_day,
    #         app_package
    #     ;
    # '''
    # print(sql2)
    # df2 = execSql(sql2)
    # df2.to_csv('/src/data/topwarAdData2.csv',index=False)

    df1 = pd.read_csv('/src/data/topwarAdData.csv')
    # df2 = pd.read_csv('/src/data/topwarAdData2.csv')

    # dfList = [df1,df2]
    dfList = [df1]
    geoList = ['KR','JP','US']
    for index in range(len(dfList)):
        df = dfList[index]
        df['cpm'] = df['cost']*1000/df['impression']
        df['install_day'] = pd.to_datetime(df['install_day'],format='%Y%m%d')

        appPackageList = df['app_package'].unique().tolist()
        for appPackage in appPackageList:
            if appPackage in ['com.topwar.gp.vn','webgameglobal']:
                continue
            for geo in geoList:
                appDf = df.loc[
                    (df['app_package']==appPackage) &
                    (df['country']==geo)
                ].copy()
                # print(appPackage,geo)
                # print(appDf['install_day'].min())
                # print(appDf['install_day'].max())
                date_range = pd.date_range(start=appDf['install_day'].min(), end=appDf['install_day'].max())
                date_df = pd.DataFrame(date_range, columns=['install_day'])
                appDf = pd.merge(date_df, appDf, on='install_day', how='left')
                appDf = appDf.fillna(0)

                appDf = appDf.sort_values(by='install_day').reset_index(drop=True)
                
                fig, axs = plt.subplots(3, 1, figsize=(10, 15))

                axs[0].plot(appDf['install_day'], appDf['impression'])
                axs[0].set_title('Impression over time')
                axs[0].set_ylabel('Impression')

                axs[1].plot(appDf['install_day'], appDf['cost'])
                axs[1].set_title('Cost over time')
                axs[1].set_ylabel('Cost')

                axs[2].plot(appDf['install_day'], appDf['cpm'])
                axs[2].set_title('CPM over time')
                axs[2].set_ylabel('CPM')

                # 自动调整子图间距
                plt.tight_layout()

                # 保存图片
                plt.savefig(f'/src/data/topwarAdData_{appPackage}_{geo}.jpg')
                plt.close()

if __name__ == '__main__':
    step1()
    step2()