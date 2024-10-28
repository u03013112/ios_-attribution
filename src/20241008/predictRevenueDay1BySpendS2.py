import os
import sys
import pandas as pd
import numpy as np

def init():
    global execSql
    global dayStr
    global appDict

    if 'o' in globals():
        print('这是线上版本')

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
                return pd_df

        execSql = execSql_online

        # 线上版本有 args 这个全局变量
        dayStr = args['dayStr']
    else:
        print('这是本地版本')
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local

        # dayStr = '20241021'  # 本地测试时的日期，可自行修改
        today = pd.to_datetime('today')
        dayStr = today.strftime('%Y%m%d')

    print(f"dayStr: {dayStr}")
    # 定义 app 的字典
    global appDict
    appDict = {'android': 'com.fun.lastwar.gp', 'ios': 'id6448786147'}

def createTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            Column(name='app', type='string', comment='app identifier'),
            Column(name='media', type='string', comment='media source'),
            Column(name='country', type='string', comment='country'),
            Column(name='install_day', type='string', comment='install day'),
            Column(name='level', type='double', comment='level (percentage change)'),
            Column(name='predicted_spend', type='double', comment='predicted spend amount'),
            Column(name='predicted_roi', type='double', comment='predicted ROI'),
        ]
        partitions = [
            Partition(name='day', type='string', comment='prediction date, like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'lastwar_predict_revenue_day1_by_spend_suggestion_result'
        o.create_table(table_name, schema, if_not_exists=True)
        print(f"表 {table_name} 创建成功或已存在。")
    else:
        print('本地版本不创建表格')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_revenue_day1_by_spend_suggestion_result'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"分区 day={dayStr} 已从表 {table_name} 中删除。")
    else:
        print('本地版本不删除分区')

def writeTable(df, dayStr):
    print('尝试将结果写入表格:')
    print(df.head(5))
    if 'o' in globals():
        table_name = 'lastwar_predict_revenue_day1_by_spend_suggestion_result'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            # 确保列的类型正确
            df['install_day'] = df['install_day'].astype(str)
            writer.write(df)
        print(f"结果已写入表 {table_name} 的分区 day={dayStr}。")
    else:
        print('writeTable 失败，o 未定义')
        print(dayStr)
        print(df)

def getSuggestionData(platform, media, country):
    global dayStr

    current_week_monday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').weekday())
    current_week_monday_str = current_week_monday.strftime('%Y%m%d')

    app = appDict[platform]
    media_condition = f"and media = '{media}'" if media != 'ALL' else "and media = 'ALL'"
    country_condition = f"and country = '{country}'" if country != 'ALL' else "and country = 'ALL'"

    sql = f'''
    select
        app,
        media,
        country,
        install_day,
        predicted_level,
        predicted_spend,
        predicted_revenue,
        predicted_roi
    from
        lastwar_predict_revenue_day1_by_spend_suggestion
    where
        day = '{current_week_monday_str}'
        and app = '{app}'
        {media_condition}
        {country_condition}
    ;
    '''
    suggestion_df = execSql(sql)
    if suggestion_df.empty:
        print("未找到符合条件的建议数据。")
        return None
    return suggestion_df

def getRoiThreshold(lastSundayStr, app, media, country):
    media_condition = f"and media = '{media}' and organic = 1" if media != 'ALL' else "and media = 'ALL'"
    country_condition = f"and country = '{country}'" if country != 'ALL' else "and country = 'ALL'"

    sql = f'''
    select
        roi_001_best
    from
        ads_predict_base_roi_day1_window_multkey
    where
        app = 502
        and type = '{app}'
        and end_date = '{lastSundayStr}'
        {media_condition}
        {country_condition}
    ;
    '''
    roi_threshold_df = execSql(sql)
    if roi_threshold_df.empty:
        print("未找到 ROI 阈值。用保守值 2% 代替。")
        return 0.02
    return roi_threshold_df.iloc[0]['roi_001_best']

def selectBestLevel(suggestion_df, roi_threshold):
    # 按照 predicted_level 从高到低排序
    suggestion_df = suggestion_df.sort_values(by='predicted_level', ascending=False)
    # 获取所有 unique 的 predicted_level，并按降序排列
    levels = suggestion_df['predicted_level'].unique()
    levels.sort()
    levels = levels[::-1]  # reverse to descending order

    # 遍历每个 level
    for level in levels:
        group = suggestion_df[suggestion_df['predicted_level'] == level]
        roi = group['predicted_roi'].iloc[0]
        if roi >= roi_threshold:
            return group
    # 如果没有满足条件的档位，返回 None
    return None

def getLastWeekData(platform, media='ALL', country='ALL'):
    global dayStr
    end_date = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').weekday()+1)
    start_date = end_date - pd.Timedelta(days=6)

    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')

    table_name = 'tmp_lw_cost_and_roi_by_day' if platform == 'android' else 'tmp_lw_cost_and_roi_by_day_ios'

    # 媒体映射，从模型表的媒体名称映射为实际数据表中的媒体名称
    media_mapping = {
        'FACEBOOK': 'Facebook Ads',
        'GOOGLE': 'googleadwords_int',
        'APPLOVIN': 'applovin_int',
        'ALL': 'ALL'
    }
    mapped_media = media_mapping.get(media, media)

    media_condition = f"and mediasource = '{mapped_media}'" if mapped_media != 'ALL' else ""
    country_condition = f"and country = '{country}'" if country != 'ALL' else ""

    sql = f'''
    select
        sum(usd) as total_spend,
        sum(d1) as total_revenue
    from
        {table_name}
    where
        install_day between '{start_date_str}' and '{end_date_str}'
        {media_condition}
        {country_condition}
    ;
    '''
    data = execSql(sql)
    if data.empty:
        print("未获取到上周的数据。")
        return None, None

    total_spend = data['total_spend'].iloc[0]
    total_revenue = data['total_revenue'].iloc[0]
    return total_spend, total_revenue

def getLastNWeekMapeData(dayStr, n=1):
    currentMonday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').weekday())
    lastMonday = currentMonday - pd.Timedelta(days=7)
    lastNWeekMonday = currentMonday - pd.Timedelta(days=7*n)

    sql = f'''
select
    app,
    media,
    country,
    avg(mape_week) as mape_week
from lastwar_predict_revenue_day1_by_spend_verification
where day between {lastNWeekMonday.strftime('%Y%m%d')} and {lastMonday.strftime('%Y%m%d')}
group by app, media, country
;
    '''
    print(sql)

    return execSql(sql)


def main(group_by_media=False, group_by_country=False, all_results=None, reports=None):
    print('group_by_media:',group_by_media)
    print('group_by_country:',group_by_country)

    global dayStr

    if all_results is None:
        all_results = []
    if reports is None:
        reports = []

    currentMonday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr,format='%Y%m%d').weekday())
    currentMondayStr = currentMonday.strftime('%Y%m%d')

    lastSunday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr,format='%Y%m%d').weekday()+1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    last_week_monday = lastSunday - pd.Timedelta(days=6)
    last_week_monday_str = last_week_monday.strftime('%Y%m%d')
    last_week_sunday_str = lastSundayStr

    platformList = ['android', 'ios']
    appDict = {'android': 'com.fun.lastwar.gp', 'ios': 'id6448786147'}

    last8WeekMapeDf = getLastNWeekMapeData(dayStr, 8)

    if 'o' not in globals():
        from src.report.feishu.feishu import sendMessageDebug
        last8WeekMapeList = last8WeekMapeDf.to_dict(orient='records')
        mapeStr = f'lastwar {dayStr} 最近8周 周平均 MAPE：\n'
        for row in last8WeekMapeList:
            mapeStr += f"{row['app']} {row['media']} {row['country']} {row['mape_week']*100:.2f}%\n"
        sendMessageDebug(mapeStr)

        # 获取不可靠的媒体和国家列表
        unreliableMediaAndCountryList = last8WeekMapeDf[last8WeekMapeDf['mape_week'] > .2].to_dict(orient='records')
        mapeStr = f'lastwar {dayStr} 最近8周 不可靠的媒体和国家列表：\n'
        for row in unreliableMediaAndCountryList:
            mapeStr += f"{row['app']} {row['media']} {row['country']} {row['mape_week']*100:.2f}%\n"
        sendMessageDebug(mapeStr)

    unreliableMediaAndCountryList = last8WeekMapeDf[last8WeekMapeDf['mape_week'] > .2][['app','media','country']].to_dict(orient='records')

    for platform in platformList:

        if platform == 'ios' and group_by_media == True:
            print('iOS 平台不支持按媒体分组')
            continue

        app = appDict[platform]
        mediaList = ['ALL']
        countryList = ['ALL']

        if group_by_media or group_by_country:
            # 从建议表中获取媒体和国家列表
            media_condition = "" if group_by_media else "and media = 'ALL'"
            country_condition = "" if group_by_country else "and country = 'ALL'"

            sql_models = f'''
            select distinct
                media,
                country
            from
                lastwar_predict_revenue_day1_by_spend_suggestion
            where
                day = '{currentMondayStr}'
                and app = '{app}'
                {media_condition}
                {country_condition}
            '''
            print(sql_models)
            models_df = execSql(sql_models)

            if group_by_media:
                mediaList = models_df['media'].unique().tolist()
                mediaList = [media if media else 'ALL' for media in mediaList]
                # 排除 'ALL'，避免重复
                mediaList = [media for media in mediaList if media != 'ALL']

            if group_by_country:
                countryList = models_df['country'].unique().tolist()
                countryList = [country if country else 'ALL' for country in countryList]
                countryList = [country for country in countryList if country != 'ALL']

            # 如果列表为空，则设置为 ['ALL']
            if not mediaList:
                mediaList = ['ALL']
            if not countryList:
                countryList = ['ALL']

        print(f"平台：{platform}，媒体列表：{mediaList}，国家列表：{countryList}")
        # continue

        for media in mediaList:
            for country in countryList:
                if {'app':app,'media': media, 'country': country} in unreliableMediaAndCountryList:
                    print(f"跳过不可靠的媒体和国家组合：{media} - {country}")
                    continue

                print(f"\n处理平台：{platform}，媒体：{media}，国家：{country}")
                # 获取建议数据
                suggestion_df = getSuggestionData(platform, media, country)
                if suggestion_df is None:
                    print("获取建议数据失败，跳过此组合。")
                    continue

                # 获取 ROI 阈值（倒推 1 日 ROI）
                roi_threshold = getRoiThreshold(lastSundayStr, app, media, country)
                print(f"倒推 1 日 ROI（roi_threshold）：{roi_threshold*100:.2f}%")
                # 为了后续选择档位时使用保守的 ROI 阈值，可以在这里调整
                conservative_roi_threshold = roi_threshold * 1.05
                print(f"保守的 ROI 阈值：{conservative_roi_threshold*100:.2f}%")

                print('所有建议数据：')
                print(suggestion_df.groupby(['predicted_level', 'predicted_roi']).sum())

                # 选择最佳档位
                best_level_df = selectBestLevel(suggestion_df, conservative_roi_threshold)
                if best_level_df is not None:
                    total_spend = best_level_df['predicted_spend'].sum()
                    predicted_roi = best_level_df['predicted_roi'].iloc[0]
                    predicted_level = best_level_df['predicted_level'].iloc[0]
                    percentage_change = predicted_level * 100  # 修改此处，直接乘以 100
                    increase_or_decrease = "增长" if percentage_change > 0 else "降低"

                    # 获取上周的实际花费和收入，计算上周的实际 ROI
                    last_week_spend, last_week_revenue = getLastWeekData(platform, media, country)
                    if last_week_spend is not None and last_week_spend != 0:
                        last_week_roi = last_week_revenue / last_week_spend
                    else:
                        last_week_roi = 0
                    print(f"上周花费：{last_week_spend:.2f}，上周收入：{last_week_revenue:.2f}，上周 ROI：{last_week_roi*100:.2f}%")

                    # 收集结果
                    for idx, row in best_level_df.iterrows():
                        result = {
                            'app': app,
                            'media': media,
                            'country': country,
                            'install_day': row['install_day'],
                            'level': row['predicted_level'],
                            'predicted_spend': row['predicted_spend'],
                            'predicted_roi': row['predicted_roi']
                        }
                        all_results.append(result)

                    # 更新报告行，按照新的格式
                    if percentage_change <= 0.01 or percentage_change >= -0.01:
                        report_line = f"{platform.upper()} 媒体：{media} 国家：{country} \n    建议总花费 {total_spend:.2f} 美元（与上周相比 保持不变），预计 ROI：{predicted_roi*100:.2f}%。\n    上周 ROI：{last_week_roi*100:.2f}% ，倒推 1 日 ROI：{roi_threshold*100:.2f}%。\n"    
                    else:
                        report_line = f"{platform.upper()} 媒体：{media} 国家：{country} \n    建议总花费 {total_spend:.2f} 美元（与上周相比{increase_or_decrease}{abs(percentage_change):.0f}%），预计 ROI：{predicted_roi*100:.2f}%。\n    上周 ROI：{last_week_roi*100:.2f}% ，倒推 1 日 ROI：{roi_threshold*100:.2f}%。\n"
                    reports.append(report_line)
                else:
                    # 如果没有满足条件的档位，不做任何建议
                    print("未找到满足条件的档位。")
                    continue
                    # 如果没有满足条件的档位，建议适度减少花费，不提供预计 ROI
                    report_line = f"{platform.upper()} 媒体：{media} 国家：{country} \n    建议适度减少花费。"

                    # 获取上周的实际花费和收入，计算上周的实际 ROI
                    last_week_spend, last_week_revenue = getLastWeekData(platform, media, country)
                    if last_week_spend is not None and last_week_spend != 0:
                        last_week_roi = last_week_revenue / last_week_spend
                    else:
                        last_week_roi = 0
                    print(f"上周花费：{last_week_spend:.2f}，上周收入：{last_week_revenue:.2f}，上周 ROI：{last_week_roi*100:.2f}%")

                    # 添加上周 ROI 和倒推 1 日 ROI 到报告
                    report_line += f"\n    上周 ROI：{last_week_roi*100:.2f}% ，倒推 1 日 ROI：{roi_threshold*100:.2f}%。\n"
                    reports.append(report_line)

def run_all():
    createTable()
    deletePartition(dayStr)
    all_results = []
    reports = []

    # 计算当前周的起始和结束日期，用于报告
    current_date = pd.to_datetime(dayStr, format='%Y%m%d')
    current_week_monday = current_date - pd.Timedelta(days=current_date.weekday())
    current_week_sunday = current_week_monday + pd.Timedelta(days=6)
    start_date_str = current_week_monday.strftime('%Y-%m-%d')
    end_date_str = current_week_sunday.strftime('%Y-%m-%d')

    reports.append(f"{start_date_str} 至 {end_date_str}\n")

    # 大盘
    reports.append('**大盘：**\n')
    main(False, False, all_results, reports)
    # 分媒体
    reports.append('**分媒体：**\n')
    main(True, False, all_results, reports)
    # 分国家（如果需要可以取消注释）
    reports.append('**分国家：**\n')
    main(False, True, all_results, reports)
    # 分媒体国家（如果需要可以取消注释）
    reports.append('**分媒体国家：**\n')
    main(True, True, all_results, reports)

    # 打印汇总报告
    if reports:
        print(f"\nLastwar 花费建议：\n")
        for line in reports:
            print(line)
        
        # 添加飞书报告
        if 'o' not in globals():
            from src.report.feishu.feishu import sendMessageDebug, sendMessage,getTenantAccessToken
            message = '**Lastwar 花费建议：**\n\n'
            message += ('\n'.join(reports))
            # sendMessageDebug(message)
            token = getTenantAccessToken()
            sendMessage(token,message,'oc_bc74e631c1d907b76d11bc511403c2e0')
    else:
        print("没有生成任何报告。")

    if all_results:
        results_df = pd.DataFrame(all_results)
        writeTable(results_df, dayStr)
    else:
        print("没有需要写入的结果。")

if __name__ == "__main__":
    init()
    run_all()
