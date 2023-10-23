# 由于之前发现2日付费金额与7日付费金额有较强的相关性，所以可以粗略认为用2日付费金额来拟合7日付费金额是比较容易实现并且准确率较高的
# 但是为了证明2日数据足够即时，2日的预测是有价值的，需要证明2日的时候进行预算调整至少是比目前的情况要好的
# 所以要在历史数据中找到连续性的真实ROI偏高或者偏低的时间区间（至少3天）
# 主要观察偏高的情况，与预算变化的情况做比对，观察目前的情况是否有及时的预算变化，如果从2日数据发现趋势，是否比目前的情况更加即时

# 所以目前的执行步骤是：
# 1、获得2023-01-01到2023-10-01的数据。包括2日付费金额，7日付费金额，按照安装日期汇总
# 2、获得对应日期的广告花费，按天汇总
# 3、计算7日ROI，按照安装日期汇总
# 4、画图观察
#  a、2日付费金额与7日付费金额画在一张图上，并画7日付费金额比2日付费金额的比值
#  b、7日ROI与广告花费画在一张图上，并给7日ROI定一个标准，比如7%，横着画一条线，方便找到ROI偏高或者偏低的时间区间。
#  c、尝试对7日付费金额与广告花费做一下平滑处理，rolling3日和rolling7日，为了观察趋势，可能需要moving average，然后再画上面b的图
# 对上面的图进行观察，有必要的话可以对一些主要的时间区间进行详细再分析

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

def getData1():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp >= UNIX_TIMESTAMP(datetime '2023-01-01 00:00:00')
        ),
        ods_platform_appsflyer_events AS (
            SELECT
                customer_user_id,
                event_timestamp,
                event_revenue_usd
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '20230101'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                o.event_timestamp,
                o.event_revenue_usd
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
            WHERE
                o.event_timestamp >= t.install_timestamp
        )
        SELECT
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 2 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_2d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_7d
        FROM
            joined_data
        GROUP BY
            install_date
        ORDER BY
            install_date;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('20231009mind2_data1'),index=False)

# getData1()

def getData2():
    sql = '''
        select
            day as install_date,
            sum(cost) as cost
        from
            (
                select
                    day,
                    mediasource,
                    getapppackagev2(
                        app,
                        mediasource,
                        campaign_name,
                        adset_name,
                        ad_name
                    ) as app_package,
                    campaign_name,
                    adset_name,
                    ad_name,
                    impressions,
                    clicks,
                    installs,
                    cost
                from
                    ods_realtime_mediasource_cost
                where
                    app = 102
                    and day >= 20230101
                    and day < 20231001
            )
        where
            app_package = 'id1479198816'
            and mediasource = 'bytedanceglobal_int'
        group by
            day;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('20231009mind2_data2'),index=False)

# getData2()

def getData3():
    sql = '''
        select 
            to_char(to_date(install_date,'yyyy-mm-dd'),'yyyymmdd') as install_date,
            sum(skad_revenue) as revenue
        from ods_platform_appsflyer_skad_details
        where
            app_id="id1479198816"
            and day >= '20230101'
            and media_source = 'bytedanceglobal_int'
        group by
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('20231009mind2_data3'),index=False)

# getData3()

def mergeData1AndData2():
    df1 = pd.read_csv(getFilename('20231009mind2_data1'))
    df2 = pd.read_csv(getFilename('20231009mind2_data2'))
    df = pd.merge(df1,df2,on='install_date',how='left')
    df = df.sort_values(by='install_date').reset_index(drop=True)
    df.to_csv(getFilename('20231009mind2_data'),index=False)

# mergeData1AndData2()

def mergeData3():
    df = pd.read_csv(getFilename('20231009mind2_data'))
    df3 = pd.read_csv(getFilename('20231009mind2_data3'))
    df = pd.merge(df,df3,on='install_date',how='left')
    df = df.sort_values(by='install_date').reset_index(drop=True)
    df = df.loc[df['install_date'] < 20231001]
    df = df.rename(columns={'revenue':'skad_revenue'})
    df.to_csv(getFilename('20231009mind2_data'),index=False)

# mergeData3()

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

def draw1():
    df = pd.read_csv(getFilename('20231009mind2_data'))
    # 过滤掉2023-02-01之前的数据，因为这个时间点之前的数据太高，影响了后面的图
    df = df.loc[df['install_date'] >= 20230201]

    df['install_date'] = pd.to_datetime(df['install_date'], format='%Y%m%d')
    df.set_index('install_date', inplace=True)

    # 拥有列和范例行
    # install_date,revenue_2d,revenue_7d,cost
    # 20230101,2797.0570172349708,6150.1081599427125,398499.47000000026

    # 画图，画3张图，竖着拼在一张大图里，其中x坐标一致，均为install_date，由于install_date比较密，所以在图例上显示的是月份
    # 图1：双y轴，其中左侧y轴：revenue_2d,revenue_7d，右侧y轴：revenue_7d/revenue_2d
    # 图2：双y轴，其中左侧y轴：revenue_7d/cost，并在左侧y轴的坐标系中横着画一条虚线，在7%的位置，右侧y轴：cost
    # 图3与图2基本一致，但是revenue_7d要换成revenue_7d的7日移动平均，cost也要换成cost的7日移动平均
    # 最后将3张图拼在一起，保存到文件中'/src/data/zk2/20231009mind2_draw1.png'
    
    fig, axs = plt.subplots(3, 1, figsize=(48, 15))
    # 图1
    ax1 = axs[0]
    ax1.plot(df.index, df['revenue_2d'], label='revenue_2d')
    ax1.plot(df.index, df['revenue_7d'], label='revenue_7d')
    ax1.legend(loc='upper left')
    ax1.set_ylabel('Revenue')

    ax1b = ax1.twinx()
    ax1b.plot(df.index, df['revenue_7d']/df['revenue_2d'], label='Ratio', color='green', linestyle='--')
    ax1b.legend(loc='upper right')
    ax1b.set_ylabel('Ratio')

    # ROI的标准值
    roiStd = 0.04

    # 图2
    ax2 = axs[1]
    ax2.plot(df.index, df['revenue_7d']/df['cost'], label='roi7', color='red')
    ax2.plot(df.index, df['skad_revenue']/df['cost'], label='skad roi', color='green')
    ax2.axhline(y=roiStd, color='r', linestyle='--')
    ax2.legend(loc='upper left')
    ax2.set_ylabel('Ratio')

    ax2b = ax2.twinx()
    ax2b.plot(df.index, df['cost'], label='Cost', color='blue')
    ax2b.legend(loc='upper right')
    ax2b.set_ylabel('Cost')

    # 图3
    df['revenue_7d_ma'] = df['revenue_7d'].ewm(span=7).mean()
    df['cost_ma'] = df['cost'].ewm(span=7).mean()

    ax3 = axs[2]
    ax3.plot(df.index, df['revenue_7d_ma']/df['cost_ma'], label='Ratio', color='red')
    ax3.axhline(y=roiStd, color='r', linestyle='--')
    ax3.legend(loc='upper left')
    ax3.set_ylabel('Ratio')

    ax3b = ax3.twinx()
    ax3b.plot(df.index, df['cost_ma'], label='Cost', color='blue')
    ax3b.legend(loc='upper right')
    ax3b.set_ylabel('Cost')

    # 设置日期格式
    date_format = DateFormatter("%m")
    for ax in axs:
        ax.xaxis.set_major_formatter(date_format)

    plt.tight_layout()
    plt.savefig('/src/data/zk2/20231009mind2_draw1.png')
    plt.show()

# draw1()

def debug01():
    df = pd.read_csv(getFilename('20231009mind2_data'))
    df['install_date'] = df['install_date'].astype(str)
    df = df.sort_values(by='install_date').reset_index(drop=True)

    dateList = []
    for i in range(1,10):
        date = {
            'name':'2023年%02d月'%i,
            'start':'2023%02d01'%i,
            'end':'2023%02d01'%(i+1)
        }
        dateList.append(date)

    plt.subplots(figsize=(16, 6))
    for date in dateList:
        df0 = df[(df['install_date'] >= date['start']) & (df['install_date'] < date['end'])]
        corr = df0.corr()
        print(date['name'],'3日收入相关系数',corr['skad_revenue']['revenue_3d'])
        print(date['name'],'7日收入相关系数',corr['skad_revenue']['revenue_7d'])
        print('')
        # 画图1
        # install_date作为x
        # revenue_3d,revenue_7d,skad_revenue作为y
        # 保存到文件中'/src/data/zk2/20231009mind2_draw1_%s.png'%date['name']
        df0['install_date'] = pd.to_datetime(df0['install_date'], format='%Y%m%d')

        plt.plot(df0['install_date'], df0['revenue_3d'], label='revenue_3d')
        plt.plot(df0['install_date'], df0['revenue_7d'], label='revenue_7d')
        plt.plot(df0['install_date'], df0['skad_revenue'], label='skad_revenue')
        plt.xlabel('install_date')
        plt.ylabel('revenue')
        plt.title('Graph 1: Revenue vs Install Date')
        plt.legend()
        plt.savefig('/src/data/zk2/20231009mind2_draw1_%s.png' % date['name'])
        plt.clf()

    print('rolling 7')
    df1 = df.copy()
    df1['revenue_3d rolling'] = df1['revenue_3d'].rolling(7).mean()
    df1['revenue_7d rolling'] = df1['revenue_7d'].rolling(7).mean()
    df1['skad_revenue rolling'] = df1['skad_revenue'].rolling(7).mean()
    for date in dateList:
        df0 = df1[(df1['install_date'] >= date['start']) & (df1['install_date'] < date['end'])]
        corr = df0.corr()
        print(date['name'],'3日收入相关系数',corr['skad_revenue rolling']['revenue_3d rolling'])
        print(date['name'],'7日收入相关系数',corr['skad_revenue rolling']['revenue_7d rolling'])
        print('')
        # # 画图2
        # # 与画图1类似，但是y轴换成rolling之后的值
        # # 保存到文件中'/src/data/zk2/20231009mind2_draw2_%s.png'%date['name']
        # df0['install_date'] = pd.to_datetime(df0['install_date'], format='%Y%m%d')

        # plt.plot(df0['install_date'], df0['revenue_3d rolling'], label='revenue_3d rolling')
        # plt.plot(df0['install_date'], df0['revenue_7d rolling'], label='revenue_7d rolling')
        # plt.plot(df0['install_date'], df0['skad_revenue rolling'], label='skad_revenue rolling')
        # plt.xlabel('install_date')
        # plt.ylabel('revenue rolling')
        # plt.title('Graph 2: Rolling Revenue vs Install Date')
        # plt.legend()
        # plt.savefig('/src/data/zk2/20231009mind2_draw2_%s.png' % date['name'])
        # plt.clf()

    

# debug01()

def debug01_2():
    hourDf = pd.read_csv(getFilename('20230919_3_sum_all'))
    hourDf = hourDf[['install_date','9H_revenue_usd','13H_revenue_usd']]
    df = pd.read_csv(getFilename('20231009mind2_data'))
    df = df.merge(hourDf,on='install_date',how='left')

    df['install_date'] = df['install_date'].astype(str)
    df = df.sort_values(by='install_date').reset_index(drop=True)

    dateList = []
    for i in range(1,10):
        date = {
            'name':'2023年%02d月'%i,
            'start':'2023%02d01'%i,
            'end':'2023%02d01'%(i+1)
        }
        dateList.append(date)

    plt.subplots(figsize=(16, 6))
    for date in dateList:
        df0 = df[(df['install_date'] >= date['start']) & (df['install_date'] < date['end'])]
        corr = df0.corr(method='spearman')
        print(date['name'],'10小时相关系数',corr['revenue_3d']['9H_revenue_usd'])
        print(date['name'],'14小时相关系数',corr['revenue_3d']['13H_revenue_usd'])
        print('')
        
        # df0['install_date'] = pd.to_datetime(df0['install_date'], format='%Y%m%d')

        # plt.plot(df0['install_date'], df0['revenue_3d'], label='revenue_3d')
        # plt.plot(df0['install_date'], df0['9H_revenue_usd'], label='9H_revenue_usd')
        # plt.plot(df0['install_date'], df0['13H_revenue_usd'], label='13H_revenue_usd')
        # plt.xlabel('install_date')
        # plt.ylabel('revenue')
        # plt.title('revenue 3day vs 10hour and 14hour')
        # plt.legend()
        # plt.savefig('/src/data/zk2/20231009mind2_draw3_%s.png' % date['name'])
        # plt.clf()

    print('rolling 7')
    df1 = df.copy()
    df1['revenue_3d rolling'] = df1['revenue_3d'].rolling(7).mean()
    df1['9H_revenue_usd rolling'] = df1['9H_revenue_usd'].rolling(7).mean()
    df1['13H_revenue_usd rolling'] = df1['13H_revenue_usd'].rolling(7).mean()

    for date in dateList:
        df0 = df1[(df1['install_date'] >= date['start']) & (df1['install_date'] < date['end'])]
        corr = df0.corr(method='spearman')
        print(date['name'],'10小时相关系数',corr['revenue_3d rolling']['9H_revenue_usd rolling'])
        print(date['name'],'14小时相关系数',corr['revenue_3d rolling']['13H_revenue_usd rolling'])
        print('')
        
        # df0['install_date'] = pd.to_datetime(df0['install_date'], format='%Y%m%d')

        # plt.plot(df0['install_date'], df0['revenue_3d rolling'], label='revenue_3d rolling')
        # plt.plot(df0['install_date'], df0['9H_revenue_usd rolling'], label='9H_revenue_usd rolling')
        # plt.plot(df0['install_date'], df0['13H_revenue_usd rolling'], label='13H_revenue_usd rolling')
        # plt.xlabel('install_date')
        # plt.ylabel('revenue rolling')
        # plt.title('revenue 3day vs 10hour and 14hour rolling 7')
        # plt.legend()
        # plt.savefig('/src/data/zk2/20231009mind2_draw4_%s.png' % date['name'])
        # plt.clf()

# debug01_2()

def debug02():
    df = pd.read_csv(getFilename('20231009mind2_data'))
    df = df.sort_values(by='install_date').reset_index(drop=True)

    N = 3
    X = 0.04

    df['roi7'] = df['revenue_7d']/df['cost']

    # 找到连续N天ROI大于X的时间区间，并将结构按照{'start':install_date,'end':install_date}的格式保存到ret中
    ret = []
    count = 0
    start_date = None
    for i in range(len(df)):
        if df.loc[i, 'roi7'] > X:
            count += 1
            if count == 1:
                start_date = df.loc[i, 'install_date']
        else:
            if count >= N:
                ret.append({'start': start_date, 'end': df.loc[i-1, 'install_date'],'count':count})
            count = 0
            start_date = None

    # 处理最后一个区间
    if count >= N:
        ret.append({'start': start_date, 'end': df.loc[len(df)-1, 'install_date']})

    return ret

def debug03():
    df = pd.read_csv(getFilename('20231009mind2_data'))
    df = df.sort_values(by='install_date').reset_index(drop=True)

    N = 3
    X = 0.04

    df['roi7'] = df['revenue_7d']/df['cost']

    # 找到连续N天ROI大于X的时间区间，并将结构按照{'start':install_date,'end':install_date}的格式保存到ret中
    ret = []
    count = 0
    start_date = None
    for i in range(len(df)):
        if df.loc[i, 'roi7'] < X:
            count += 1
            if count == 1:
                start_date = df.loc[i, 'install_date']
        else:
            if count >= N:
                ret.append({'start': start_date, 'end': df.loc[i-1, 'install_date'],'count':count})
            count = 0
            start_date = None

    # 处理最后一个区间
    if count >= N:
        ret.append({'start': start_date, 'end': df.loc[len(df)-1, 'install_date']})

    return ret

# ret = debug02()
# # ret = debug03()
# for r in ret:
#     print(r)

from analyze import makeLevels1,makeCvMap,addCv
def debug04():
    # 查看每个月的不同付费区间用户分布走势
    # 暂时不考虑完全不付费用户（7日内）
    df = pd.read_csv(getFilename('20230919_analyze3'))

    levels = makeLevels1(df,usd='revenue_1d',N=3)
    cvMap = makeCvMap(levels)
    print(cvMap)
    df = addCv(df,cvMap,usd='revenue_1d',cv='cv')
    df = df.groupby(['install_date','cv']).agg({'game_uid':'count'}).reset_index()
    df['install_date'] = pd.to_datetime(df['install_date'], format='%Y%m%d')

    # 计算每一天的每一种cv的占比
    df['cv_ratio'] = df.groupby('install_date')['game_uid'].apply(lambda x: x / x.sum())

    # fig, axs = plt.subplots(4, 1, figsize=(24, 24), sharex=True)
    # cv_values = df['cv'].unique()
    # for i in range(4):
    #     df_cv = df[df['cv'] == cv_values[i]]
    #     axs[i].plot(df_cv['install_date'], df_cv['cv_ratio'], label=f'CV {cv_values[i]}')
    #     axs[i].set_ylabel('CV Ratio')
    #     axs[i].legend()

    # axs[3].set_xlabel('Install Date')
    # plt.savefig('/src/data/zk2/20231009mind2_draw4_1.png')
    # plt.clf()

    # # 计算滚动7天的game_uid
    # df['rolling7_game_uid'] = df.groupby('cv')['game_uid'].rolling(window=7).sum().reset_index(drop=True)

    # # 计算滚动7天后的cv占比
    # df['rolling7_cv_ratio'] = df.groupby('install_date')['rolling7_game_uid'].apply(lambda x: x / x.sum())

    # # 绘制滚动7天后的cv占比图表
    # fig, axs = plt.subplots(4, 1, figsize=(24, 24), sharex=True)
    # cv_values = df['cv'].unique()
    # for i in range(4):
    #     df_cv = df[df['cv'] == cv_values[i]]
    #     axs[i].plot(df_cv['install_date'], df_cv['rolling7_cv_ratio'], label=f'CV {cv_values[i]}')
    #     axs[i].set_ylabel('Rolling 7 Days CV Ratio')
    #     axs[i].legend()

    # axs[3].set_xlabel('Install Date')
    # plt.savefig('/src/data/zk2/20231009mind2_draw4_2.png')
    # plt.clf()

    # 转换日期格式并添加月份列
    df['install_date'] = pd.to_datetime(df['install_date'], format='%Y%m%d')
    df['month'] = df['install_date'].dt.to_period('M')

    # 按月份和cv分组，计算每个cv的总数
    df_monthly = df.groupby(['month', 'cv'])['game_uid'].sum().reset_index()

    # 计算每个月每个cv的占比
    df_monthly['cv_ratio'] = df_monthly.groupby('month')['game_uid'].apply(lambda x: x / x.sum())

    df_monthly = df_monthly.sort_values(by=['cv','month']).reset_index(drop=True)
    # 打印结果
    print(df_monthly)

# debug04()

def getData5():
    sql = '''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid,
                country_code
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp >= UNIX_TIMESTAMP(datetime '2023-01-01 00:00:00')
        ),
        ods_platform_appsflyer_events AS (
            SELECT
                customer_user_id,
                event_timestamp,
                event_revenue_usd
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '20230101'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                t.country_code,
                o.event_timestamp,
                o.event_revenue_usd
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
                AND o.event_timestamp >= t.install_timestamp
        )
        SELECT
            game_uid,
            country_code,
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 2 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_2d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_7d
        FROM
            joined_data
        GROUP BY
            install_date,
            game_uid,
            country_code
        HAVING
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END
            ) > 0
        ORDER BY
            install_date;
    '''
    print(sql)
    df = execSql(sql)
    return df

# getData5().to_csv('/src/data/zk2/20230919_analyze5.csv',index=False)

def debug5():
    df = pd.read_csv(getFilename('20230919_analyze5'))
    df2 = df.groupby( by = ['country_code']).agg({'game_uid':'count'}).reset_index()
    df2 = df2.sort_values(by='game_uid',ascending=False).reset_index(drop=True)
    # print(df2)
    # 只保留前5个国家，剩下的都改为other
    countryList = df2['country_code'].tolist()
    countryList = countryList[:10]
    
    df.loc[df['country_code'].isin(countryList) == False,'country_code'] = 'other'
    
    df['install_date'] = pd.to_datetime(df['install_date'], format='%Y%m%d')
    df['month'] = df['install_date'].dt.to_period('M')
    df = df.groupby(by = ['month','country_code']).agg({'game_uid':'count'}).reset_index()
    df['pay rate'] = df.groupby(['month'])['game_uid'].apply(lambda x: x / x.sum())
    df = df.sort_values(by=['month','game_uid'],ascending=False).reset_index(drop=True)
    print(df)


debug5()