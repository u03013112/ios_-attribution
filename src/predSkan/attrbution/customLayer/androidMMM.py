# 用安卓数据进行MMM尝试，只是类似，自建模型，没有那么复杂
# 不再使用SKAN数据，原因是SKAN数据的安装时间偏差大，只能用均线来做，没有价值。另外2023-02-28更换了CV Map，导致CV值变化。

# 暂时选用媒体数据：花费（美元），展示数，点击数，安装数
# 选用用户数据：7日付费金额总额（分媒体，用于验算）

# 步骤
# 1、获取媒体数据
# 2、获取用户数据
# 3、数据整理
# 4、建立模型，并训练
# 5、记录日志

import datetime
import pandas as pd

import os
import sys
import numpy as np
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename):
    return '/src/data/customLayer/%s.csv'%(filename)

# 1、获取媒体数据
def getMediaData():
    sql = '''
        select
            mediasource as media,
            to_char(
                to_date(day, "yyyymmdd"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(installs) as installs,
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
                and day >= 20220501
                and day < 20230228
        )
        where
            app_package = 'com.topwar.gp'
        group by
            mediasource,
            day
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('media_20220501_20230227'),index=False)
    return df

def loadMediaData():
    df = pd.read_csv(getFilename('media_20220501_20230227'))
    return df

# 2、获取用户数据
def getUserData():
    # 从AF事件表中获取用户数据
    # 安装日期在2022-05-01~2023-02-27之间
    # 用户7日内付费金额
    # 海外安卓
    # 用af id做用户区分
    # 按照安装日期（天）与媒体进行汇总
    sql = '''
        WITH install_data AS (
        SELECT
            appsflyer_id,
            media_source,
            to_char(
            to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
            "yyyy-mm-dd"
            ) AS install_date
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'com.topwar.gp'
            AND zone = 0
            AND event_name = 'install'
            AND `day` BETWEEN '20220501'
            AND '20230227'
        )
        SELECT
        install_data.install_date,
        install_data.media_source,
        COUNT(
            DISTINCT ods_platform_appsflyer_events.appsflyer_id
        ) AS user_count,
        SUM(
            CASE
            WHEN event_name = 'af_purchase' THEN event_revenue_usd
            ELSE 0
            END
        ) AS revenue_7d
        FROM
        ods_platform_appsflyer_events
        JOIN install_data ON ods_platform_appsflyer_events.appsflyer_id = install_data.appsflyer_id
        WHERE
        app_id = 'com.topwar.gp'
        AND zone = 0
        AND event_name = 'af_purchase'
        AND ods_platform_appsflyer_events.`day` BETWEEN '20220501'
        AND '20230227'
        GROUP BY
        install_data.install_date,
        install_data.media_source;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('user_20220501_20230227'),index=False)
    return df

def loadUserData():
    df = pd.read_csv(getFilename('user_20220501_20230227'))
    return df

# 3、数据整理
def data():
    mediaDf = loadMediaData()
    userDf = loadUserData()
    # mediaDf head
    # media,install_date,impressions,clicks,installs,cost
    # googleadwords_int,2022-06-26,4464073,60158,14485,58545.160183
    # bytedanceglobal_int,2022-07-01,4863548,74629,6098,41180.009999999995
    # bytedanceglobal_int,2022-12-02,2577609,16161,2942,5855.0999999999985
    # googleadwords_int,2023-02-25,12751384,66414,16585,172018.28122000003
    # googleadwords_int,2022-07-05,5123034,61063,14832,74051.356882
    # bilibili_int,2023-01-19,0,0,0,0.0
    # sinaweibo_int,2022-08-10,33735,52,7,200.0
    # baiduyuansheng_int,2023-01-01,819410,5404,1496,14751.579999999998
    # googleadwords_int,2022-06-28,7110427,77293,17364,78999.56730699999

    # userDf head
    # install_date,media_source,user_count,revenue_7d
    # 2022-05-01,Facebook Ads,177,1718.2606950476006
    # 2022-05-01,applovin_int,3,23.02929799980481
    # 2022-05-01,bytedanceglobal_int,96,1047.2647974666934
    # 2022-05-01,googleadwords_int,314,5873.826107844505
    # 2022-05-01,ironsource_int,15,104.47545783444497
    # 2022-05-01,moloco_int,1,20.6194653997699
    # 2022-05-01,restricted,22,489.2903333874733
    # 2022-05-01,unityads_int,3,4.877042383861182
    # 2022-05-02,Facebook Ads,128,1496.5599624708673

    # 要求
    # 将媒体进行分组，googleadwords_int，bytedanceglobal_int，Facebook Ads和其他
    # 制作X，按照上面分组顺序，每个媒体的impressions,clicks,installs,cost，共4组
    # 制作Y，按照安装日期汇总，计算7日回收
    # 制作分媒体每天7日回收，用于模型验算


# 4、建立模型，并训练
if __name__ == '__main__':
    # getMediaData()
    # loadMediaData()

    getUserData()
    loadUserData()
