# 海外iOS周报，获得一周内的趋势
import os
import datetime
import pandas as pd
import numpy as np
import subprocess
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.report.geo import getIOSGeoGroup01
from src.report.media import getIOSMediaGroup01

def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)

headStr = '''
---
CJKmainfont: WenQuanYi Zen Hei
---

---
header-includes:
  - \\usepackage{color}
  - \\usepackage{xcolor}
  - \\usepackage{placeins}
---

'''

# 获得目前的UTC0日期，格式20231018
today = datetime.datetime.utcnow()
todayStr = today.strftime('%Y%m%d')

# for test
todayStr = '20231025'
today = datetime.datetime.strptime(todayStr,'%Y%m%d')

print('今日日期：',todayStr)
# 获得一周前的UTC0日期，格式20231011
startDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
endDayStr = todayStr
directory = f'/src/data/report/iOSWeekly{startDayStr}_{endDayStr}'

# 获得指定日期的数据
# 获得广告花费，分媒体、分国家，按照安装日期汇总
def getAdCostData(startDayStr,endDayStr):
    filename = getFilename(f'adCost{startDayStr}_{endDayStr}')
    print('getAdCostData:',filename)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        print('从MC获得数据')
    sql = f'''
        SELECT
            install_day as day,
            mediasource as media_source,
            country as country_code,
            sum(cost_value_usd) as cost
        FROM
            (
                SELECT
                    install_day,
                    mediasource,
                    country,
                    cost_value_usd
                FROM
                    rg_bi.dwd_overseas_cost_new
                WHERE
                    app = '102'
                    AND zone = '0'
                    AND app_package = 'id1479198816'
                    AND cost_value_usd > 0
                UNION
                ALL
                SELECT
                    install_day,
                    mediasource,
                    country,
                    cost_value_usd
                FROM
                    rg_bi.dwd_overseas_cost_history
                WHERE
                    app = '102'
                    AND zone = '0'
                    AND app_package = 'id1479198816'
                    AND cost_value_usd > 0
            ) AS combined_table
        WHERE
            install_day BETWEEN '{startDayStr}' AND '{endDayStr}'
        group by
            install_day,
            mediasource,
            country;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(filename,index=False)
    print('已获得%d条数据'%len(df))
    print('存储在%s'%filename)
    return df

# 获得1日收入、3日收入、7日收入，分媒体、分国家，按照安装日期汇总
def getDataFromMC(startDayStr,endDayStr):

    filename = getFilename(f'20231006Data1_{startDayStr}_{endDayStr}')
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        print('从MC获得数据')

    # startDayStr 格式 20231001 转成 2023-10-01 00:00:00
    startDayStr2 = datetime.datetime.strptime(startDayStr,'%Y%m%d').strftime('%Y-%m-%d 00:00:00')
    # endDayStr 格式 20231001 转成 2023-10-01 23:59:59
    endDayStr2 = datetime.datetime.strptime(endDayStr,'%Y%m%d').strftime('%Y-%m-%d 23:59:59')

    sql = f'''
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
                AND install_timestamp between UNIX_TIMESTAMP(datetime '{startDayStr2}') AND UNIX_TIMESTAMP(datetime '{endDayStr2}')
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
                AND day >= '{startDayStr}'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        adv_uid_mutidays AS (
            SELECT
                customer_user_id,
                facebook_ads_rate,
                googleadwords_int_rate,
                bytedanceglobal_int_rate,
                other_rate
            FROM
                rg_bi.topwar_ios_funplus02_adv_uid_mutidays
            WHERE
                day between '{startDayStr}' AND '{endDayStr}'
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                t.country_code,
                o.event_timestamp,
                o.event_revenue_usd,
                COALESCE(a.facebook_ads_rate, 0) AS facebook_ads_rate,
                COALESCE(a.googleadwords_int_rate, 0) AS googleadwords_int_rate,
                COALESCE(a.bytedanceglobal_int_rate, 0) AS bytedanceglobal_int_rate,
                COALESCE(a.other_rate, 0) AS other_rate
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
                AND o.event_timestamp >= t.install_timestamp
                LEFT JOIN adv_uid_mutidays a ON t.game_uid = a.customer_user_id
        )
        SELECT
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
                END * facebook_ads_rate
            ) AS facebook_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END * googleadwords_int_rate
            ) AS google_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END * bytedanceglobal_int_rate
            ) AS bytedanceglobal_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END * other_rate
            ) AS other_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd * (1 - facebook_ads_rate - googleadwords_int_rate - bytedanceglobal_int_rate - other_rate)
                    ELSE 0
                END
            ) AS organic_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * facebook_ads_rate
            ) AS facebook_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * googleadwords_int_rate
            ) AS google_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * bytedanceglobal_int_rate
            ) AS bytedanceglobal_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * other_rate
            ) AS other_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd * (1 - facebook_ads_rate - googleadwords_int_rate - bytedanceglobal_int_rate - other_rate)
                    ELSE 0
                END
            ) AS organic_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * facebook_ads_rate
            ) AS facebook_revenue_7d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * googleadwords_int_rate
            ) AS google_revenue_7d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * bytedanceglobal_int_rate
            ) AS bytedanceglobal_revenue_7d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * other_rate
            ) AS other_revenue_7d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd * (1 - facebook_ads_rate - googleadwords_int_rate - bytedanceglobal_int_rate - other_rate)
                    ELSE 0
                END
            ) AS organic_revenue_7d
        FROM
            joined_data
        GROUP BY
            install_date,
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
    df.to_csv(filename,index=False)
    print('已获得%d条数据'%len(df))
    print('存储在%s'%filename)
    return df

# 数据格式修改
def getDataFromMC2(df):
    # df = pd.read_csv(getFilename(f'20231006Data1_{startDayStr}_{endDayStr}'))
    # 将宽格式的DataFrame转换为长格式
    df_long = df.melt(id_vars=['country_code', 'install_date'], 
                    var_name='media_day', 
                    value_name='revenue')

    # 分割media_day列为media和day列
    df_long[['media', 'day']] = df_long['media_day'].str.split('_', n=1,expand=True)

    # 删除不再需要的media_day列
    df_long = df_long.drop(columns='media_day')

    # 使用pivot_table函数将其转换为你需要的格式
    df_pivot = df_long.pivot_table(index=['country_code', 'install_date', 'media'], 
                                columns='day', 
                                values='revenue').reset_index()

    # 重命名列名
    df_pivot.columns.name = ''
    df_pivot = df_pivot.rename(columns={'1d': 'revenue_1d', '3d': 'revenue_3d', '7d': 'revenue_7d'})
    # df_pivot['install_date'] = df['install_date'].astype(str)
    # print(df_pivot)
    # df_pivot.to_csv(getFilename(f'20231006Data2_{startDayStr}_{endDayStr}'),index=False)
    return df_pivot
    

def macdAnalysis(df,target='ROI_1d',startDayStr='20231001',endDayStr='20231007', analysisDayCount=7,picFilenamePrefix=''):
    # print('macdAnalysis:',startDayStr,endDayStr,analysisDayCount)
    # 画图
    df = df.copy()
    df.sort_values(by=['install_date'], inplace=True)

    df['EMA12'] = df[target].ewm(span=12).mean()
    df['EMA26'] = df[target].ewm(span=26).mean()

    # 计算MACD值
    df['MACD'] = df['EMA12'] - df['EMA26']

    # 计算9日EMA作为信号线
    df['Signal'] = df['MACD'].ewm(span=9).mean()

    # 选择最近两周的数据（升序排序后的前14行）
    last_draw_days = df.loc[(df['install_date']>=startDayStr) & (df['install_date']<=endDayStr)]

    # 将install_date转换为datetime格式
    df['install_date'] = pd.to_datetime(df['install_date'])
    
    # 创建一个画布，包含两个子图
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6, 6), sharex=True)

    # 绘制ROI走势图
    ax1.plot(last_draw_days['install_date'], last_draw_days[target], label='ROI', color='b')
    ax1.legend(loc='upper left')
    ax1.set_ylabel('ROI Value')
    ax1.set_title(f'{target} Trend ({startDayStr}~{endDayStr})')
    ax1.grid()

    # 绘制MACD和信号线
    ax2.plot(last_draw_days['install_date'], last_draw_days['MACD'], label='MACD', color='b')
    ax2.plot(last_draw_days['install_date'], last_draw_days['Signal'], label='Signal', color='r')
    ax2.legend(loc='upper left')
    ax2.set_xlabel('Install Date')
    ax2.set_ylabel('MACD Value')
    ax2.set_title(f'MACD Trend for {target} ({startDayStr}~{endDayStr})')
    ax2.grid()
    # 设置x轴刻度标签的旋转角度
    plt.xticks(rotation=45)
    # 保存图像到文件
    picFilename = getFilename(f'{picFilenamePrefix}_{startDayStr}_{endDayStr}_{target}_macd', 'png')
    plt.savefig(picFilename, dpi=300, bbox_inches='tight')
    plt.close()

    # 分析最近analysisDayCount天的MACD趋势
    reportStr = ''
    last_analysis_days = df.loc[(df['install_date'] >= startDayStr) & (df['install_date'] <= endDayStr)].tail(analysisDayCount)
    macd_cross = (last_analysis_days['MACD'] - last_analysis_days['Signal'])

    # 计算每天的趋势，存到安装日期，趋势这样的表中，每天一个趋势，按照安装日期升序
    daily_trend = pd.DataFrame()
    daily_trend['install_date'] = last_analysis_days['install_date']
    daily_trend['trend'] = macd_cross > 0
    daily_trend['trend'] = daily_trend['trend'].apply(lambda x: "\\protect\\textcolor{red}{上涨}" if x else "\\protect\\textcolor{green}{下跌}")

    # 从第一行遍历，记录初始趋势，每次找到趋势与初始趋势不一致的时候停止，输出需要的结果
    reportStr += "#### 趋势分析（MACD分析）\n\n"
    
    reportStr += f'最近{analysisDayCount}天的主要趋势：\n\n'
    start_date = daily_trend['install_date'].iloc[0]
    initial_trend = daily_trend['trend'].iloc[0]
    for i in range(1, len(daily_trend)):
        current_trend = daily_trend['trend'].iloc[i]
        if current_trend != initial_trend:
            end_date = daily_trend['install_date'].iloc[i - 1]
            if start_date != end_date:
                reportStr += f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')} {initial_trend}\n\n"
            start_date = daily_trend['install_date'].iloc[i]
            initial_trend = current_trend
    reportStr += f"{start_date.strftime('%Y-%m-%d')}~{daily_trend['install_date'].iloc[-1].strftime('%Y-%m-%d')} {initial_trend}\n\n\n"

    reportStr += "\\textbf{针对最近趋势进行展望：}\n\n"
    reportStr += f"({start_date.strftime('%Y-%m-%d')}~{daily_trend['install_date'].iloc[-1].strftime('%Y-%m-%d')})"
    # 对最后一波趋势进行细化分析
    last_trend_duration = (daily_trend['install_date'].iloc[-1] - start_date).days + 1
    if last_trend_duration <= 1:
        reportStr += "\n这是一个波动期。没有明显趋势。"
    else:
        reportStr += f"\n这是一个持续{initial_trend}趋势。"
        macd_signal_diff = last_analysis_days['MACD'] - last_analysis_days['Signal']
        if abs(macd_signal_diff.iloc[-1]) - abs(macd_signal_diff.iloc[-2]) < 0:
            reportStr += "但是趋势有减弱情况，这个趋势可能即将结束。"
        else:
            reportStr += "趋势还在增强，这个趋势可能仍将继续。"
        reportStr += '\n\n'
    # 

    reportStr += r'''
\begin{figure}[!h]
    \centering
    \includegraphics{''' + f'./{picFilenamePrefix}_{startDayStr}_{endDayStr}_{target}_macd.png'+'''}
\end{figure}
\FloatBarrier
    '''

    return reportStr

# 获得两周内的ROI趋势分析
def getROIReport():
    reportStr = '## 大盘ROI分析\n\n'
    # 大致内容如下：

    # 大盘情况
    # 上周的首日、3日、7日ROI
    # 按照安装日期汇总
    # 整周的均值
    # 环比、同比
    # MACD趋势

    # 为了获得可以使用的环比、同比数据，以及MACD趋势，需要获得最近60天的数据
    startDayStr = (today - datetime.timedelta(days=60)).strftime('%Y%m%d')
    endDayStr = todayStr

    adCostDf = getAdCostData(startDayStr,endDayStr)
    adCostDf.rename(columns={'day':'install_date'},inplace=True)
    adCostDf.loc[adCostDf.media_source == 'facebook_ads','media_source'] = 'facebook'
    adCostDf.loc[adCostDf.media_source == 'googleadwords_int','media_source'] = 'google'
    adCostDf.loc[adCostDf.media_source == 'bytedanceglobal_int','media_source'] = 'bytedanceglobal'
    adCostDf.loc[~adCostDf.media_source.isin(['facebook','google','bytedanceglobal']),'media_source'] = 'other'
    adCostDf.rename(columns={'media_source':'media'},inplace=True)
    adCostDf = adCostDf.groupby(['install_date','country_code','media'],as_index=False).agg({'cost':'sum'})
    adCostDf['install_date'] = adCostDf['install_date'].astype(str)
    
    df = getDataFromMC(startDayStr,endDayStr)
    df = getDataFromMC2(df)
    df['install_date'] = df['install_date'].astype(str)
    df = df.merge(adCostDf,on=['country_code','install_date','media'],how='outer').fillna(0)

    # 为了获得同比，需要获得去年同期的数据
    startDayStr2 = (today - datetime.timedelta(days=60+365)).strftime('%Y%m%d')
    endDayStr2 = (today - datetime.timedelta(days=365)).strftime('%Y%m%d')
    adCostDf2 = getAdCostData(startDayStr2,endDayStr2)
    adCostDf2.rename(columns={'day':'install_date'},inplace=True)
    adCostDf2.loc[adCostDf2.media_source == 'facebook_ads','media_source'] = 'facebook'
    adCostDf2.loc[adCostDf2.media_source == 'googleadwords_int','media_source'] = 'google'
    adCostDf2.loc[adCostDf2.media_source == 'bytedanceglobal_int','media_source'] = 'bytedanceglobal'
    adCostDf2.loc[~adCostDf2.media_source.isin(['facebook','google','bytedanceglobal']),'media_source'] = 'other'
    adCostDf2.rename(columns={'media_source':'media'},inplace=True)
    adCostDf2 = adCostDf2.groupby(['install_date','country_code','media'],as_index=False).agg({'cost':'sum'})

    df2 = getDataFromMC(startDayStr2,endDayStr2)
    df2 = getDataFromMC2(df2)
    df2 = df2.merge(adCostDf2,on=['country_code','install_date','media'],how='outer').fillna(0)

    # 合并成大盘，group by install_date,country_code列忽略，其他列求和
    totalDf = df.groupby(['install_date']).agg({
        'revenue_1d':'sum',
        'revenue_3d':'sum',
        'revenue_7d':'sum',
        'cost':'sum'
    }).reset_index()
    totalDf = totalDf.sort_values(by=['install_date'],ascending=False).reset_index(drop=True)
    totalDf['install_date'] = totalDf['install_date'].astype(str)
    
    # 同比大盘
    totalDf2 = df2.groupby(['install_date']).agg({
        'revenue_1d':'sum',
        'revenue_3d':'sum',
        'revenue_7d':'sum',
        'cost':'sum'
    }).reset_index()
    totalDf2 = totalDf2.sort_values(by=['install_date'],ascending=False).reset_index(drop=True)
    totalDf2['install_date'] = totalDf2['install_date'].astype(str)


    reportStr += '### 1日ROI 大盘\n\n'
    # 本周的首日、3日、7日ROI的均值，其中3日和7日都可能是不完整的
    # 所以获取最近7天的数据作为本周1日数据，做环比、同比
    day1StartDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
    day1EndDayStr = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
    day1Df = totalDf.loc[(totalDf.install_date >= day1StartDayStr) & (totalDf.install_date <= day1EndDayStr)].copy()
    day1ROIMean = day1Df.revenue_1d.sum() / day1Df.cost.sum()
    # 环比
    day1QoQStartDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
    day1QoQEndDayStr = (today - datetime.timedelta(days=8)).strftime('%Y%m%d')
    day1QoQDf = totalDf.loc[(totalDf.install_date >= day1QoQStartDayStr) & (totalDf.install_date <= day1QoQEndDayStr)].copy()
    day1QoQROIMean = day1QoQDf.revenue_1d.sum() / day1QoQDf.cost.sum()
    
    reportStr += '%s~%s均值：\\textbf{%.2f\\%%}\n\n'%(day1StartDayStr,day1EndDayStr,day1ROIMean*100)
    # 环比差异
    QoQRate = (day1ROIMean - day1QoQROIMean) / day1QoQROIMean
    QoQColor = 'red'
    if QoQRate < 0:
        QoQColor = 'green'
    reportStr += '环比%s~%s均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day1QoQStartDayStr,day1QoQEndDayStr,day1QoQROIMean*100,QoQColor,QoQRate*100)
    
    # 同比
    day1YoYStartDayStr = (today - datetime.timedelta(days=365+7)).strftime('%Y%m%d')
    day1YoYEndDayStr = (today - datetime.timedelta(days=365+1)).strftime('%Y%m%d')
    day1YoYDf = totalDf2.loc[(totalDf2.install_date >= day1YoYStartDayStr) & (totalDf2.install_date <= day1YoYEndDayStr)].copy()
    day1YoYROIMean = day1YoYDf.revenue_1d.sum() / day1YoYDf.cost.sum()
    # 同比差异
    YoYRate = (day1ROIMean - day1YoYROIMean) / day1YoYROIMean
    YoYColor = 'red'
    if YoYRate < 0:
        YoYColor = 'green'
    reportStr += '同比%s~%s均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day1YoYStartDayStr,day1YoYEndDayStr,day1YoYROIMean*100,YoYColor,YoYRate*100)
    
    # MACD
    # 计算1日ROI
    totalDf['ROI_1d'] = totalDf['revenue_1d'] / totalDf['cost']
    macdReport = macdAnalysis(totalDf,target='ROI_1d',startDayStr=day1QoQStartDayStr,endDayStr=day1EndDayStr,analysisDayCount=14,picFilenamePrefix='total')
    reportStr += macdReport + '\n\n'
    # ------------------------------------1日ROI 大盘 结束------------------------------------

    # 然后获取最近T-3~t-10天的数据作为本周3日数据，做环比、同比
    reportStr += '### 3日ROI 大盘\n\n'
    day3StartDayStr = (today - datetime.timedelta(days=10)).strftime('%Y%m%d')
    day3EndDayStr = (today - datetime.timedelta(days=3)).strftime('%Y%m%d')
    day3Df = totalDf.loc[(totalDf.install_date >= day3StartDayStr) & (totalDf.install_date <= day3EndDayStr)].copy()
    day3ROIMean = day3Df.revenue_3d.sum() / day3Df.cost.sum()
    # 环比
    day3QoQStartDayStr = (today - datetime.timedelta(days=17)).strftime('%Y%m%d')
    day3QoQEndDayStr = (today - datetime.timedelta(days=10)).strftime('%Y%m%d')
    day3QoQDf = totalDf.loc[(totalDf.install_date >= day3QoQStartDayStr) & (totalDf.install_date <= day3QoQEndDayStr)].copy()
    day3QoQROIMean = day3QoQDf.revenue_3d.sum() / day3QoQDf.cost.sum()

    reportStr += '%s~%s均值：\\textbf{%.2f\\%%}\n\n'%(day3StartDayStr,day3EndDayStr,day3ROIMean*100)
    # 环比差异
    QoQRate = (day3ROIMean - day3QoQROIMean) / day3QoQROIMean
    QoQColor = 'red'
    if QoQRate < 0:
        QoQColor = 'green'
    reportStr += '环比%s~%s均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day3QoQStartDayStr,day3QoQEndDayStr,day3QoQROIMean*100,QoQColor,QoQRate*100)

    # 同比
    day3YoYStartDayStr = (today - datetime.timedelta(days=365+10)).strftime('%Y%m%d')
    day3YoYEndDayStr = (today - datetime.timedelta(days=365+3)).strftime('%Y%m%d')
    day3YoYDf = totalDf2.loc[(totalDf2.install_date >= day3YoYStartDayStr) & (totalDf2.install_date <= day3YoYEndDayStr)].copy()
    day3YoYROIMean = day3YoYDf.revenue_3d.sum() / day3YoYDf.cost.sum()
    # 同比差异
    YoYRate = (day3ROIMean - day3YoYROIMean) / day3YoYROIMean
    YoYColor = 'red'
    if YoYRate < 0:
        YoYColor = 'green'
    reportStr += '同比%s~%s均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day3YoYStartDayStr,day3YoYEndDayStr,day3YoYROIMean*100,YoYColor,YoYRate*100)

    # MACD
    # 计算3日ROI
    totalDf['ROI_3d'] = totalDf['revenue_3d'] / totalDf['cost']
    macdReport = macdAnalysis(totalDf,target='ROI_3d',startDayStr=day3QoQStartDayStr,endDayStr=day3EndDayStr,picFilenamePrefix='total')
    reportStr += macdReport + '\n\n'
    # ------------------------------------3日ROI 大盘 结束------------------------------------

    # 然后获取最近T-7~t-14天的数据作为本周7日数据，做环比、同比
    reportStr += '### 7日ROI 大盘\n\n'
    day7StartDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
    day7EndDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
    day7Df = totalDf.loc[(totalDf.install_date >= day7StartDayStr) & (totalDf.install_date <= day7EndDayStr)].copy()
    day7ROIMean = day7Df.revenue_7d.sum() / day7Df.cost.sum()
    # 环比
    day7QoQStartDayStr = (today - datetime.timedelta(days=21)).strftime('%Y%m%d')
    day7QoQEndDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
    day7QoQDf = totalDf.loc[(totalDf.install_date >= day7QoQStartDayStr) & (totalDf.install_date <= day7QoQEndDayStr)].copy()
    day7QoQROIMean = day7QoQDf.revenue_7d.sum() / day7QoQDf.cost.sum()

    reportStr += '%s~%s均值：\\textbf{%.2f\\%%}\n\n'%(day7StartDayStr,day7EndDayStr,day7ROIMean*100)
    # 环比差异
    QoQRate = (day7ROIMean - day7QoQROIMean) / day7QoQROIMean
    QoQColor = 'red'
    if QoQRate < 0:
        QoQColor = 'green'
    reportStr += '环比%s~%s均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day7QoQStartDayStr,day7QoQEndDayStr,day7QoQROIMean*100,QoQColor,QoQRate*100)

    # 同比
    day7YoYStartDayStr = (today - datetime.timedelta(days=365+14)).strftime('%Y%m%d')
    day7YoYEndDayStr = (today - datetime.timedelta(days=365+7)).strftime('%Y%m%d')
    day7YoYDf = totalDf2.loc[(totalDf2.install_date >= day7YoYStartDayStr) & (totalDf2.install_date <= day7YoYEndDayStr)].copy()
    day7YoYROIMean = day7YoYDf.revenue_7d.sum() / day7YoYDf.cost.sum()
    # 同比差异
    YoYRate = (day7ROIMean - day7YoYROIMean) / day7YoYROIMean
    YoYColor = 'red'
    if YoYRate < 0:
        YoYColor = 'green'
    reportStr += '同比%s~%s均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day7YoYStartDayStr,day7YoYEndDayStr,day7YoYROIMean*100,YoYColor,YoYRate*100)

    # MACD
    # 计算7日ROI
    totalDf['ROI_7d'] = totalDf['revenue_7d'] / totalDf['cost']
    macdReport = macdAnalysis(totalDf,target='ROI_7d',startDayStr=day7QoQStartDayStr,endDayStr=day7EndDayStr,picFilenamePrefix='total')
    reportStr += macdReport + '\n\n'
    # ------------------------------------7日ROI 大盘 结束------------------------------------



    # 分国家的上述情况
    # 融合归因分媒体的上述情况

    # 本周内容由于没有足够的7日数据，只关注首日、3日ROI。其中3日也是不完整的，需要处理数据的时候注意一下

    return reportStr

def getROIReportGroupByCountry():
    reportStr = '## 国家ROI分析\n\n'
    startDayStr = (today - datetime.timedelta(days=60)).strftime('%Y%m%d')
    endDayStr = todayStr

    geoGroupList = getIOSGeoGroup01()

    adCostDf = getAdCostData(startDayStr,endDayStr)
    adCostDf.rename(columns={'day':'install_date'},inplace=True)
    adCostDf.loc[adCostDf.media_source == 'Facebook Ads','media_source'] = 'facebook'
    adCostDf.loc[adCostDf.media_source == 'googleadwords_int','media_source'] = 'google'
    adCostDf.loc[adCostDf.media_source == 'bytedanceglobal_int','media_source'] = 'bytedanceglobal'
    # 将所有media_source == 'tiktokglobal_int' 的行删掉，这个不知道是干嘛的
    adCostDf = adCostDf.loc[adCostDf.media_source != 'tiktokglobal_int']

    adCostDf.loc[~adCostDf.media_source.isin(['facebook','google','bytedanceglobal']),'media_source'] = 'other'
    adCostDf.rename(columns={'media_source':'media'},inplace=True)
    
    adCostDf['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        adCostDf.loc[adCostDf.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    adCostDf = adCostDf.groupby(['install_date','geoGroup','media'],as_index=False).agg({'cost':'sum'})
    

    df = getDataFromMC(startDayStr,endDayStr)
    df = getDataFromMC2(df)
    df['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df.loc[df.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    
    df = df.groupby(['install_date','geoGroup','media'],as_index=False).sum().reset_index(drop=True)
    df = df.merge(adCostDf,on=['geoGroup','install_date','media'],how='outer').fillna(0)

    # 为了获得同比，需要获得去年同期的数据
    startDayStr2 = (today - datetime.timedelta(days=60+365)).strftime('%Y%m%d')
    endDayStr2 = (today - datetime.timedelta(days=365)).strftime('%Y%m%d')
    adCostDf2 = getAdCostData(startDayStr2,endDayStr2)
    adCostDf2.rename(columns={'day':'install_date'},inplace=True)
    adCostDf2.loc[adCostDf2.media_source == 'facebook_ads','media_source'] = 'facebook'
    adCostDf2.loc[adCostDf2.media_source == 'googleadwords_int','media_source'] = 'google'
    adCostDf2.loc[adCostDf2.media_source == 'bytedanceglobal_int','media_source'] = 'bytedanceglobal'
    adCostDf2.loc[~adCostDf2.media_source.isin(['facebook','google','bytedanceglobal']),'media_source'] = 'other'
    adCostDf2.rename(columns={'media_source':'media'},inplace=True)
    adCostDf2['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        adCostDf2.loc[adCostDf2.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    adCostDf2 = adCostDf2.groupby(['install_date','geoGroup','media'],as_index=False).agg({'cost':'sum'})

    df2 = getDataFromMC(startDayStr2,endDayStr2)
    df2 = getDataFromMC2(df2)
    df2['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df2.loc[df2.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    df2 = df2.groupby(['install_date','geoGroup','media'],as_index=False).sum().reset_index(drop=True)
    df2 = df2.merge(adCostDf2,on=['geoGroup','install_date','media'],how='outer').fillna(0)

    totalDf = df.groupby(['install_date','geoGroup']).agg({
        'revenue_1d':'sum',
        'revenue_3d':'sum',
        'revenue_7d':'sum',
        'cost':'sum'
    }).reset_index()
    totalDf = totalDf.sort_values(by=['install_date','geoGroup'],ascending=False).reset_index(drop=True)
    totalDf['install_date'] = totalDf['install_date'].astype(str)

    totalDf2 = df2.groupby(['install_date','geoGroup']).agg({
        'revenue_1d':'sum',
        'revenue_3d':'sum',
        'revenue_7d':'sum',
        'cost':'sum'
    }).reset_index()
    totalDf2 = totalDf2.sort_values(by=['install_date','geoGroup'],ascending=False).reset_index(drop=True)
    totalDf2['install_date'] = totalDf2['install_date'].astype(str)

    reportStr += '### 花费占比 分国家\n\n'
    day1StartDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
    day1EndDayStr = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
    day1QoQStartDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
    day1QoQEndDayStr = (today - datetime.timedelta(days=8)).strftime('%Y%m%d')
    day1YoYStartDayStr = (today - datetime.timedelta(days=365+7)).strftime('%Y%m%d')
    day1YoYEndDayStr = (today - datetime.timedelta(days=365+1)).strftime('%Y%m%d')
    for geoGroup in geoGroupList:
        reportStr += '\\textbf{%s}\n\n'%geoGroup['name']
        day1Df = totalDf.loc[(totalDf.install_date >= day1StartDayStr) & (totalDf.install_date <= day1EndDayStr) & (totalDf.geoGroup == geoGroup['name'])].copy()

        # 计算这个国家的花费占比，即这个国家的花费 / 总花费
        totalCostDf = totalDf.loc[(totalDf.install_date >= day1StartDayStr) & (totalDf.install_date <= day1EndDayStr)].copy()
        totalCostDf = totalCostDf.groupby(['install_date']).agg({'cost':'sum'}).reset_index()
        totalCostDf = totalCostDf[['install_date','cost']]
        day1Df = day1Df.merge(totalCostDf,on=['install_date'],how='left',suffixes=('','_total'))
        day1Df['cost_rate'] = day1Df['cost'] / day1Df['cost_total']
        day1CostRate = day1Df['cost'].sum()/day1Df['cost_total'].sum()

        reportStr += '%s~%s花费占比%.2f%%\n\n'%(day1StartDayStr,day1EndDayStr,day1CostRate*100)

        # 环比
        totalCostDf = totalDf.loc[(totalDf.install_date >= day1QoQStartDayStr) & (totalDf.install_date <= day1QoQEndDayStr)].copy()
        totalCostDf = totalCostDf.groupby(['install_date']).agg({'cost':'sum'}).reset_index()
        totalCostDf = totalCostDf[['install_date','cost']]
        day1QoQDf = totalDf.loc[(totalDf.install_date >= day1QoQStartDayStr) & (totalDf.install_date <= day1QoQEndDayStr) & (totalDf.geoGroup == geoGroup['name'])].copy()
        day1QoQDf = day1QoQDf.merge(totalCostDf,on=['install_date'],how='left',suffixes=('','_total'))
        day1QoQDf['cost_rate'] = day1QoQDf['cost'] / day1QoQDf['cost_total']
        day1QoQCostRate = day1QoQDf['cost'].sum()/day1QoQDf['cost_total'].sum()
        QoQCostRate = (day1CostRate - day1QoQCostRate) / day1QoQCostRate
        QoQCostColor = 'red'
        if QoQCostRate < 0:
            QoQCostColor = 'green'
        reportStr += '环比%s~%s花费占比:\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day1QoQStartDayStr,day1QoQEndDayStr,day1QoQCostRate*100,QoQCostColor,QoQCostRate*100)

        # 同比
        totalCostDf = totalDf2.loc[(totalDf2.install_date >= day1YoYStartDayStr) & (totalDf2.install_date <= day1YoYEndDayStr)].copy()
        totalCostDf = totalCostDf.groupby(['install_date']).agg({'cost':'sum'}).reset_index()
        totalCostDf = totalCostDf[['install_date','cost']]
        day1YoYDf = totalDf2.loc[(totalDf2.install_date >= day1YoYStartDayStr) & (totalDf2.install_date <= day1YoYEndDayStr) & (totalDf2.geoGroup == geoGroup['name'])].copy()
        day1YoYDf = day1YoYDf.merge(totalCostDf,on=['install_date'],how='left',suffixes=('','_total'))
        day1YoYDf['cost_rate'] = day1YoYDf['cost'] / day1YoYDf['cost_total']
        day1YoYCostRate = day1YoYDf['cost'].sum()/day1YoYDf['cost_total'].sum()
        YoYCostRate = (day1CostRate - day1YoYCostRate) / day1YoYCostRate
        # print(geoGroup['name'],day1YoYCostRate,day1YoYDf['cost'].sum(),day1YoYDf['cost_total'].sum())
        YoYCostColor = 'red'
        if YoYCostRate < 0:
            YoYCostColor = 'green'
        reportStr += '同比%s~%s花费占比:\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day1YoYStartDayStr,day1YoYEndDayStr,day1YoYCostRate*100,YoYCostColor,YoYCostRate*100)

        # 画cost_rate图
        day1Df = day1Df.sort_values(by=['install_date'],ascending=True).reset_index(drop=True)
        plt.figure(figsize=(6,3))
        plt.plot(day1Df['install_date'],day1Df['cost_rate'])
        plt.xticks(rotation=45)
        plt.title(f'{geoGroup["name"]} cost rate')
        plt.tight_layout()
        plt.grid()
        picFilename = getFilename(f'./{geoGroup["name"]}_{day1StartDayStr}_{day1EndDayStr}_cost_rate', 'png')
        plt.savefig(picFilename)
        plt.close()
        reportStr += r'''
\begin{figure}[!h]
    \centering
    \includegraphics{''' + f'./{geoGroup["name"]}_{day1StartDayStr}_{day1EndDayStr}_cost_rate.png'+'''}
\end{figure}
\FloatBarrier
    '''

    reportStr += '\n\n### 1日ROI 国家\n\n'
    day1StartDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
    day1EndDayStr = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
    day1QoQStartDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
    day1QoQEndDayStr = (today - datetime.timedelta(days=8)).strftime('%Y%m%d')
    day1YoYStartDayStr = (today - datetime.timedelta(days=365+7)).strftime('%Y%m%d')
    day1YoYEndDayStr = (today - datetime.timedelta(days=365+1)).strftime('%Y%m%d')
    for geoGroup in geoGroupList:
        reportStr += f'#### {geoGroup["name"]}\n\n'
        day1Df = totalDf.loc[(totalDf.install_date >= day1StartDayStr) & (totalDf.install_date <= day1EndDayStr) & (totalDf.geoGroup == geoGroup['name'])].copy()
        day1ROIMean = day1Df.revenue_1d.sum() / day1Df.cost.sum()

        reportStr += '%s~%s ROI均值：\\textbf{%.2f\\%%}\n\n'%(day1StartDayStr,day1EndDayStr,day1ROIMean*100)

        # 环比
        day1QoQDf = totalDf.loc[(totalDf.install_date >= day1QoQStartDayStr) & (totalDf.install_date <= day1QoQEndDayStr) & (totalDf.geoGroup == geoGroup['name'])].copy()
        day1QoQROIMean = day1QoQDf.revenue_1d.sum() / day1QoQDf.cost.sum()
        
        
        # 环比差异
        QoQRate = (day1ROIMean - day1QoQROIMean) / day1QoQROIMean
        QoQColor = 'red'
        if QoQRate < 0:
            QoQColor = 'green'
        reportStr += '环比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day1QoQStartDayStr,day1QoQEndDayStr,day1QoQROIMean*100,QoQColor,QoQRate*100)

        # 同比
        day1YoYDf = totalDf2.loc[(totalDf2.install_date >= day1YoYStartDayStr) & (totalDf2.install_date <= day1YoYEndDayStr) & (totalDf2.geoGroup == geoGroup['name'])].copy()
        day1YoYROIMean = day1YoYDf.revenue_1d.sum() / day1YoYDf.cost.sum()

        # 同比差异
        YoYRate = (day1ROIMean - day1YoYROIMean) / day1YoYROIMean
        YoYColor = 'red'
        if YoYRate < 0:
            YoYColor = 'green'
        
        reportStr += '同比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day1YoYStartDayStr,day1YoYEndDayStr,day1YoYROIMean*100,YoYColor,YoYRate*100)

        # MACD
        # 计算1日ROI
        geoDf = totalDf.loc[(totalDf.geoGroup == geoGroup['name'])].copy()
        geoDf['ROI_1d'] = geoDf['revenue_1d'] / geoDf['cost']
        macdReport = macdAnalysis(geoDf,target='ROI_1d',startDayStr=day1QoQStartDayStr,endDayStr=day1EndDayStr,analysisDayCount=14,picFilenamePrefix=geoGroup["name"])
        reportStr += macdReport + '\n\n'

    # ------------------------------------1日ROI 国家 结束------------------------------------

    reportStr += '\n\n### 3日ROI 国家\n\n'
    for geoGroup in geoGroupList:
        reportStr += f'#### {geoGroup["name"]}\n\n'
        day3StartDayStr = (today - datetime.timedelta(days=10)).strftime('%Y%m%d')
        day3EndDayStr = (today - datetime.timedelta(days=3)).strftime('%Y%m%d')
        day3Df = totalDf.loc[(totalDf.install_date >= day3StartDayStr) & (totalDf.install_date <= day3EndDayStr) & (totalDf.geoGroup == geoGroup['name'])].copy()
        day3ROIMean = day3Df.revenue_3d.sum() / day3Df.cost.sum()
        # 环比
        day3QoQStartDayStr = (today - datetime.timedelta(days=17)).strftime('%Y%m%d')
        day3QoQEndDayStr = (today - datetime.timedelta(days=10)).strftime('%Y%m%d')
        day3QoQDf = totalDf.loc[(totalDf.install_date >= day3QoQStartDayStr) & (totalDf.install_date <= day3QoQEndDayStr) & (totalDf.geoGroup == geoGroup['name'])].copy()
        day3QoQROIMean = day3QoQDf.revenue_3d.sum() / day3QoQDf.cost.sum()

        reportStr += '%s~%s ROI均值：\\textbf{%.2f\\%%}\n\n'%(day3StartDayStr,day3EndDayStr,day3ROIMean*100)
        # 环比差异
        QoQRate = (day3ROIMean - day3QoQROIMean) / day3QoQROIMean
        QoQColor = 'red'
        if QoQRate < 0:
            QoQColor = 'green'
        reportStr += '环比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day3QoQStartDayStr,day3QoQEndDayStr,day3QoQROIMean*100,QoQColor,QoQRate*100)

        # 同比
        day3YoYStartDayStr = (today - datetime.timedelta(days=365+10)).strftime('%Y%m%d')
        day3YoYEndDayStr = (today - datetime.timedelta(days=365+3)).strftime('%Y%m%d')
        day3YoYDf = totalDf2.loc[(totalDf2.install_date >= day3YoYStartDayStr) & (totalDf2.install_date <= day3YoYEndDayStr) & (totalDf2.geoGroup == geoGroup['name'])].copy()
        day3YoYROIMean = day3YoYDf.revenue_3d.sum() / day3YoYDf.cost.sum()
        # 同比差异
        YoYRate = (day3ROIMean - day3YoYROIMean) / day3YoYROIMean
        YoYColor = 'red'
        if YoYRate < 0:
            YoYColor = 'green'
        reportStr += '同比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day3YoYStartDayStr,day3YoYEndDayStr,day3YoYROIMean*100,YoYColor,YoYRate*100)

        # MACD
        # 计算3日ROI
        geoDf = totalDf.loc[(totalDf.geoGroup == geoGroup['name'])].copy()
        geoDf['ROI_3d'] = geoDf['revenue_3d'] / geoDf['cost']
        macdReport = macdAnalysis(geoDf,target='ROI_3d',startDayStr=day3QoQStartDayStr,endDayStr=day3EndDayStr,picFilenamePrefix=geoGroup["name"])
        reportStr += macdReport + '\n\n'
    # ------------------------------------3日ROI 国家 结束------------------------------------

    reportStr += '\n\n### 7日ROI 国家\n\n'
    for geoGroup in geoGroupList:
        reportStr += f'#### {geoGroup["name"]}\n\n'
        day7StartDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
        day7EndDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
        day7Df = totalDf.loc[(totalDf.install_date >= day7StartDayStr) & (totalDf.install_date <= day7EndDayStr) & (totalDf.geoGroup == geoGroup['name'])].copy()
        day7ROIMean = day7Df.revenue_7d.sum() / day7Df.cost.sum()
        # 环比
        day7QoQStartDayStr = (today - datetime.timedelta(days=21)).strftime('%Y%m%d')
        day7QoQEndDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
        day7QoQDf = totalDf.loc[(totalDf.install_date >= day7QoQStartDayStr) & (totalDf.install_date <= day7QoQEndDayStr) & (totalDf.geoGroup == geoGroup['name'])].copy()
        day7QoQROIMean = day7QoQDf.revenue_7d.sum() / day7QoQDf.cost.sum()

        reportStr += '%s~%s ROI均值：\\textbf{%.2f\\%%}\n\n'%(day7StartDayStr,day7EndDayStr,day7ROIMean*100)
        # 环比差异
        QoQRate = (day7ROIMean - day7QoQROIMean) / day7QoQROIMean
        QoQColor = 'red'
        if QoQRate < 0:
            QoQColor = 'green'
        reportStr += '环比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day7QoQStartDayStr,day7QoQEndDayStr,day7QoQROIMean*100,QoQColor,QoQRate*100)

        # 同比
        day7YoYStartDayStr = (today - datetime.timedelta(days=365+14)).strftime('%Y%m%d')
        day7YoYEndDayStr = (today - datetime.timedelta(days=365+7)).strftime('%Y%m%d')
        day7YoYDf = totalDf2.loc[(totalDf2.install_date >= day7YoYStartDayStr) & (totalDf2.install_date <= day7YoYEndDayStr) & (totalDf2.geoGroup == geoGroup['name'])].copy()
        day7YoYROIMean = day7YoYDf.revenue_7d.sum() / day7YoYDf.cost.sum()
        # 同比差异
        YoYRate = (day7ROIMean - day7YoYROIMean) / day7YoYROIMean
        YoYColor = 'red'
        if YoYRate < 0:
            YoYColor = 'green'
        reportStr += '同比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day7YoYStartDayStr,day7YoYEndDayStr,day7YoYROIMean*100,YoYColor,YoYRate*100)

        # MACD
        # 计算7日ROI
        geoDf = totalDf.loc[(totalDf.geoGroup == geoGroup['name'])].copy()
        geoDf['ROI_7d'] = geoDf['revenue_7d'] / geoDf['cost']
        macdReport = macdAnalysis(geoDf,target='ROI_7d',startDayStr=day7QoQStartDayStr,endDayStr=day7EndDayStr,picFilenamePrefix=geoGroup["name"])
        reportStr += macdReport + '\n\n'
    # ------------------------------------7日ROI 国家 结束------------------------------------

    return reportStr

def getROIReportGroupByMedia():
    reportStr = '## 媒体ROI分析\n\n'
    startDayStr = (today - datetime.timedelta(days=60)).strftime('%Y%m%d')
    endDayStr = todayStr

    mediaGroupList = getIOSMediaGroup01()

    adCostDf = getAdCostData(startDayStr,endDayStr)
    adCostDf.rename(columns={'day':'install_date'},inplace=True)
    adCostDf.loc[adCostDf.media_source == 'Facebook Ads','media_source'] = 'facebook'
    adCostDf.loc[adCostDf.media_source == 'googleadwords_int','media_source'] = 'google'
    adCostDf.loc[adCostDf.media_source == 'bytedanceglobal_int','media_source'] = 'bytedanceglobal'
    # 将所有media_source == 'tiktokglobal_int' 的行删掉，这个不知道是干嘛的
    adCostDf = adCostDf.loc[adCostDf.media_source != 'tiktokglobal_int']

    adCostDf.loc[~adCostDf.media_source.isin(['facebook','google','bytedanceglobal']),'media_source'] = 'other'
    adCostDf.rename(columns={'media_source':'media'},inplace=True)
    adCostDf = adCostDf.groupby(['install_date','media'],as_index=False).agg({'cost':'sum'})

    df = getDataFromMC(startDayStr,endDayStr)
    df = getDataFromMC2(df)
    
    df = df.groupby(['install_date','media'],as_index=False).sum().reset_index(drop=True)
    df = df.merge(adCostDf,on=['install_date','media'],how='outer').fillna(0)

    totalDf = df.groupby(['install_date','media']).agg({
        'revenue_1d':'sum',
        'revenue_3d':'sum',
        'revenue_7d':'sum',
        'cost':'sum'
    }).reset_index()
    totalDf = totalDf.sort_values(by=['install_date','media'],ascending=False).reset_index(drop=True)
    totalDf['install_date'] = totalDf['install_date'].astype(str)


    # iOS分媒体采用的是融合归因数据，只有2023-04-01之后的数据，所以暂时不做同比
    reportStr += '### 花费占比 分媒体\n\n'
    day1StartDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
    day1EndDayStr = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
    day1QoQStartDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
    day1QoQEndDayStr = (today - datetime.timedelta(days=8)).strftime('%Y%m%d')
    
    for mediaGroup in mediaGroupList:
        reportStr += '\\textbf{%s}\n\n'%mediaGroup['name']
        day1Df = totalDf.loc[(totalDf.install_date >= day1StartDayStr) & (totalDf.install_date <= day1EndDayStr) & (totalDf.media.isin(mediaGroup['codeList']))].copy()
        
        # 计算这个国家的花费占比，即这个国家的花费 / 总花费
        totalCostDf = totalDf.loc[(totalDf.install_date >= day1StartDayStr) & (totalDf.install_date <= day1EndDayStr)].copy()
        totalCostDf = totalCostDf.groupby(['install_date']).agg({'cost':'sum'}).reset_index()
        totalCostDf = totalCostDf[['install_date','cost']]
        day1Df = day1Df.merge(totalCostDf,on=['install_date'],how='left',suffixes=('','_total'))
        day1Df['cost_rate'] = day1Df['cost'] / day1Df['cost_total']
        
        day1CostRate = day1Df['cost'].sum()/day1Df['cost_total'].sum()

        reportStr += '%s~%s花费占比%.2f%%\n\n'%(day1StartDayStr,day1EndDayStr,day1CostRate*100)

        # 环比
        totalCostDf = totalDf.loc[(totalDf.install_date >= day1QoQStartDayStr) & (totalDf.install_date <= day1QoQEndDayStr)].copy()
        totalCostDf = totalCostDf.groupby(['install_date']).agg({'cost':'sum'}).reset_index()
        totalCostDf = totalCostDf[['install_date','cost']]
        day1QoQDf = totalDf.loc[(totalDf.install_date >= day1QoQStartDayStr) & (totalDf.install_date <= day1QoQEndDayStr) & (totalDf.media.isin(mediaGroup['codeList']))].copy()
        day1QoQDf = day1QoQDf.merge(totalCostDf,on=['install_date'],how='left',suffixes=('','_total'))
        day1QoQDf['cost_rate'] = day1QoQDf['cost'] / day1QoQDf['cost_total']
        day1QoQCostRate = day1QoQDf['cost'].sum()/day1QoQDf['cost_total'].sum()
        QoQCostRate = (day1CostRate - day1QoQCostRate) / day1QoQCostRate
        QoQCostColor = 'red'
        if QoQCostRate < 0:
            QoQCostColor = 'green'
        reportStr += '环比%s~%s花费占比:\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day1QoQStartDayStr,day1QoQEndDayStr,day1QoQCostRate*100,QoQCostColor,QoQCostRate*100)

        # 画cost_rate图
        day1Df = day1Df.sort_values(by=['install_date'],ascending=True).reset_index(drop=True)
        plt.figure(figsize=(6,3))
        plt.plot(day1Df['install_date'],day1Df['cost_rate'])
        plt.xticks(rotation=45)
        plt.title(f'{mediaGroup["name"]} cost rate')
        plt.tight_layout()
        plt.grid()
        picFilename = getFilename(f'./{mediaGroup["name"]}_{day1StartDayStr}_{day1EndDayStr}_cost_rate', 'png')
        plt.savefig(picFilename)
        plt.close()
        reportStr += r'''
\begin{figure}[!h]
    \centering
    \includegraphics{''' + f'./{mediaGroup["name"]}_{day1StartDayStr}_{day1EndDayStr}_cost_rate.png'+'''}
\end{figure}
\FloatBarrier
    '''
        
    reportStr += '\n\n### 1日ROI 媒体\n\n'
    day1StartDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
    day1EndDayStr = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
    day1QoQStartDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
    day1QoQEndDayStr = (today - datetime.timedelta(days=8)).strftime('%Y%m%d')
    
    for mediaGroup in mediaGroupList:
        reportStr += f'#### {mediaGroup["name"]}\n\n'
        day1Df = totalDf.loc[(totalDf.install_date >= day1StartDayStr) & (totalDf.install_date <= day1EndDayStr) & (totalDf.media.isin(mediaGroup['codeList']))].copy()
        day1ROIMean = day1Df.revenue_1d.sum() / day1Df.cost.sum()

        reportStr += '%s~%s ROI均值：\\textbf{%.2f\\%%}\n\n'%(day1StartDayStr,day1EndDayStr,day1ROIMean*100)

        # 环比
        day1QoQDf = totalDf.loc[(totalDf.install_date >= day1QoQStartDayStr) & (totalDf.install_date <= day1QoQEndDayStr) & (totalDf.media.isin(mediaGroup['codeList']))].copy()
        day1QoQROIMean = day1QoQDf.revenue_1d.sum() / day1QoQDf.cost.sum()

        # 环比差异
        QoQRate = (day1ROIMean - day1QoQROIMean) / day1QoQROIMean
        QoQColor = 'red'
        if QoQRate < 0:
            QoQColor = 'green'
        reportStr += '环比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day1QoQStartDayStr,day1QoQEndDayStr,day1QoQROIMean*100,QoQColor,QoQRate*100)

        # MACD
        # 计算1日ROI
        mediaDf = totalDf.loc[(totalDf.media.isin(mediaGroup['codeList']))].copy()
        mediaDf['ROI_1d'] = mediaDf['revenue_1d'] / mediaDf['cost']
        macdReport = macdAnalysis(mediaDf,target='ROI_1d',startDayStr=day1QoQStartDayStr,endDayStr=day1EndDayStr,picFilenamePrefix=mediaGroup["name"])
        reportStr += macdReport + '\n\n'

    # ------------------------------------1日ROI 媒体 结束------------------------------------

    reportStr += '\n\n### 3日ROI 媒体\n\n'
    for mediaGroup in mediaGroupList:
        reportStr += f'#### {mediaGroup["name"]}\n\n'
        day3StartDayStr = (today - datetime.timedelta(days=10)).strftime('%Y%m%d')
        day3EndDayStr = (today - datetime.timedelta(days=3)).strftime('%Y%m%d')
        day3Df = totalDf.loc[(totalDf.install_date >= day3StartDayStr) & (totalDf.install_date <= day3EndDayStr) & (totalDf.media.isin(mediaGroup['codeList']))].copy()
        day3ROIMean = day3Df.revenue_3d.sum() / day3Df.cost.sum()
        # 环比
        day3QoQStartDayStr = (today - datetime.timedelta(days=17)).strftime('%Y%m%d')
        day3QoQEndDayStr = (today - datetime.timedelta(days=10)).strftime('%Y%m%d')

        day3QoQDf = totalDf.loc[(totalDf.install_date >= day3QoQStartDayStr) & (totalDf.install_date <= day3QoQEndDayStr) & (totalDf.media.isin(mediaGroup['codeList']))].copy()
        day3QoQROIMean = day3QoQDf.revenue_3d.sum() / day3QoQDf.cost.sum()
        
        reportStr += '%s~%s ROI均值：\\textbf{%.2f\\%%}\n\n'%(day3StartDayStr,day3EndDayStr,day3ROIMean*100)

        # 环比差异
        QoQRate = (day3ROIMean - day3QoQROIMean) / day3QoQROIMean
        QoQColor = 'red'
        if QoQRate < 0:
            QoQColor = 'green'
        reportStr += '环比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day3QoQStartDayStr,day3QoQEndDayStr,day3QoQROIMean*100,QoQColor,QoQRate*100)

        # MACD
        # 计算3日ROI
        mediaDf = totalDf.loc[(totalDf.media.isin(mediaGroup['codeList']))].copy()
        mediaDf['ROI_3d'] = mediaDf['revenue_3d'] / mediaDf['cost']
        macdReport = macdAnalysis(mediaDf,target='ROI_3d',startDayStr=day3QoQStartDayStr,endDayStr=day3EndDayStr,picFilenamePrefix=mediaGroup["name"])
        reportStr += macdReport + '\n\n'
    # ------------------------------------3日ROI 媒体 结束------------------------------------

    reportStr += '\n\n### 7日ROI 媒体\n\n'
    for mediaGroup in mediaGroupList:
        reportStr += f'#### {mediaGroup["name"]}\n\n'
        day7StartDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
        day7EndDayStr = (today - datetime.timedelta(days=7)).strftime('%Y%m%d')
        day7Df = totalDf.loc[(totalDf.install_date >= day7StartDayStr) & (totalDf.install_date <= day7EndDayStr) & (totalDf.media.isin(mediaGroup['codeList']))].copy()
        day7ROIMean = day7Df.revenue_7d.sum() / day7Df.cost.sum()
        # 环比
        day7QoQStartDayStr = (today - datetime.timedelta(days=21)).strftime('%Y%m%d')
        day7QoQEndDayStr = (today - datetime.timedelta(days=14)).strftime('%Y%m%d')
        day7QoQDf = totalDf.loc[(totalDf.install_date >= day7QoQStartDayStr) & (totalDf.install_date <= day7QoQEndDayStr) & (totalDf.media.isin(mediaGroup['codeList']))].copy()
        day7QoQROIMean = day7QoQDf.revenue_7d.sum() / day7QoQDf.cost.sum()

        reportStr += '%s~%s ROI均值：\\textbf{%.2f\\%%}\n\n'%(day7StartDayStr,day7EndDayStr,day7ROIMean*100)
        # 环比差异
        QoQRate = (day7ROIMean - day7QoQROIMean) / day7QoQROIMean
        QoQColor = 'red'
        if QoQRate < 0:
            QoQColor = 'green'
        reportStr += '环比%s~%s ROI均值：\\textbf{%.2f\\%%}(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(day7QoQStartDayStr,day7QoQEndDayStr,day7QoQROIMean*100,QoQColor,QoQRate*100)

        # MACD
        # 计算7日ROI
        mediaDf = totalDf.loc[(totalDf.media.isin(mediaGroup['codeList']))].copy()
        mediaDf['ROI_7d'] = mediaDf['revenue_7d'] / mediaDf['cost']
        macdReport = macdAnalysis(mediaDf,target='ROI_7d',startDayStr=day7QoQStartDayStr,endDayStr=day7EndDayStr,picFilenamePrefix=mediaGroup["name"])
        reportStr += macdReport + '\n\n'
    # ------------------------------------7日ROI 媒体 结束------------------------------------
    
    return reportStr
    
def toPdf(path):
    # 切换到指定目录
    os.chdir(path)

    mdFilename = 'report.md'
    pdfFilename = 'report.pdf'
    # 调用 pandoc 将 md 转换为 pdf
    # pandoc report.md -o report.pdf --pdf-engine=xelatex
    subprocess.run(['pandoc', mdFilename, '-o', pdfFilename, '--pdf-engine=xelatex'])
    print('转化为pdf成功！')
    print('保存路径：',path+'/'+pdfFilename)

def main():
    # 建立目录'/src/data/report/{todayStr}'，如果已存在则跳过
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    reportContext = headStr

    reportContext += f'# 海外iOS周报 {startDayStr}_{endDayStr}\n\n'

    # 获得两周内的ROI趋势分析
    reportContext += getROIReport()
    reportContext += getROIReportGroupByCountry()
    reportContext += getROIReportGroupByMedia()

    print(reportContext)
    with open(f'{directory}/report.md','w',encoding='utf-8') as f:
        f.write(reportContext)

    print(f'{directory}/report.md')
    toPdf(directory)
    

if __name__ == '__main__':
    main()
    # print('Done!')