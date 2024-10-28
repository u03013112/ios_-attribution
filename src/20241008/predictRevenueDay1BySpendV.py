import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json

def init():
    global execSql
    global dayStr
    global appDict

    if 'o' in globals():
        print('this is online version')

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
        dayStr = '20241013'  # 本地测试时的日期，可自行修改

    # 定义 app 的字典
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
            Column(name='revenue_d1_read', type='double', comment='Actual revenue'),
            Column(name='revenue_d1_predicted', type='double', comment='Predicted revenue'),
            Column(name='mape_day', type='double', comment='Mean Absolute Percentage Error per day'),
            Column(name='week', type='string', comment='Week'),
            Column(name='mape_week', type='double', comment='MAPE per week'),
        ]
        partitions = [
            Partition(name='day', type='string', comment='prediction date, like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'lastwar_predict_revenue_day1_by_spend_verification'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deletePartition(dayStr):
    if 'o' in globals():
        table_name = 'lastwar_predict_revenue_day1_by_spend_verification'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s' % (dayStr), if_exists=True)
        print(f"Partition day={dayStr} deleted from table {table_name}.")
    else:
        print('No partition deletion in local version')

def mapMediaName(media):
    media_mapping = {
        'facebookadnetwork': 'Facebook Ads',
        'FACEBOOK': 'Facebook Ads',
        'applovin_int': 'Applovin',
        'APPLOVIN': 'Applovin',
        'googleadwords_int': 'googleadwords_int',
        'GOOGLE': 'googleadwords_int',
        'bytedanceglobal_int': 'bytedanceglobal_int',
        'unityads': 'unityads',
        'inmobi_int': 'inmobi_int',
        'ironsource_int': 'ironsource_int',
        'mintegral_int': 'mintegral_int',
        'vungle_int': 'vungle_int',
        'Apple Search Ads': 'Apple Search Ads',
        'organic': 'Organic',
        'ALL': 'ALL'
    }
    return media_mapping.get(media, media)

def getActualData(install_day_start, install_day_end, platform, media, country):
    # 根据平台选择不同的表名
    table_name = 'tmp_lw_cost_and_roi_by_day' if platform == 'android' else 'tmp_lw_cost_and_roi_by_day_ios'
    app = appDict[platform]

    # 媒体映射，从模型表的媒体名称映射为实际数据表中的媒体名称
    media_mapping = {
        'FACEBOOK': 'Facebook Ads',
        'GOOGLE': 'googleadwords_int',
        'APPLOVIN': 'applovin_int'
    }
    mapped_media = media_mapping.get(media, media)

    # 处理媒体和国家的条件
    media_condition = f"and mediasource = '{mapped_media}'" if mapped_media != 'ALL' else ""
    country_condition = f"and country = '{country}'" if country != 'ALL' else ""

    sql = f'''
    select
        install_day,
        mediasource,
        country,
        sum(usd) as usd,
        sum(d1) as d1
    from
        {table_name}
    where
        install_day between '{install_day_start}' and '{install_day_end}'
        {media_condition}
        {country_condition}
    group by
        install_day, mediasource, country;
    '''
    print(sql)
    data = execSql(sql)
    data['app'] = app
    # 映射媒体名称到标准名称
    data['media'] = data['mediasource'].fillna('ALL')
    data['media'] = data['media'].apply(mapMediaName)
    data['country'] = data['country'].fillna('ALL')
    return data

def loadModels(mondayStr, app, media, country):
    media_condition = f"and media = '{media}'" if media != 'ALL' else "and media = 'ALL'"
    country_condition = f"and country = '{country}'" if country != 'ALL' else "and country = 'ALL'"
    sql = f'''
        select
            app,
            media,
            country,
            model
        from
            lastwar_predict_revenue_day1_by_spend
        where
            day = '{mondayStr}'
            and app = '{app}'
            {media_condition}
            {country_condition}
        '''
    print(sql)
    models_df = execSql(sql)
    if models_df.empty:
        print("No models found for the given conditions.")
        return None
    # 取出第一个模型
    row = models_df.iloc[0]
    model = model_from_json(row['model'])
    return model

def makePredictionsAndCalculateErrors(actual_data, model, app, media, country):
    results = []
    actual_data['install_day'] = pd.to_datetime(actual_data['install_day'], format='%Y%m%d')

    # 汇总数据
    group_cols = ['install_day']
    if media != 'ALL':
        group_cols.append('media')
    if country != 'ALL':
        group_cols.append('country')

    grouped_data = actual_data.groupby(group_cols).agg({
        'usd': 'sum',
        'd1': 'sum'
    }).reset_index()

    # 准备预测数据
    df_pred = grouped_data[['install_day', 'usd']].copy()
    df_pred.rename(columns={'install_day': 'ds', 'usd': 'ad_spend'}, inplace=True)
    # 进行预测
    df_pred['ad_spend'] = df_pred['ad_spend'].astype(float)
    df_pred['ds'] = pd.to_datetime(df_pred['ds'])
    print(df_pred.head(10))
    forecast = model.predict(df_pred)
    # 获取预测的收入
    df_pred['revenue_d1_predicted'] = forecast['yhat']
    # 合并实际收入和预测收入
    df_pred['revenue_d1_read'] = grouped_data['d1'].values
    # 计算 mape_day
    df_pred['mape_day'] = (df_pred['revenue_d1_predicted'] - df_pred['revenue_d1_read']).abs() / df_pred['revenue_d1_read'].replace(0, np.nan)
    df_pred['install_day'] = df_pred['ds']
    # 添加其他信息
    df_pred['app'] = app
    df_pred['media'] = media
    df_pred['country'] = country
    results.append(df_pred[['install_day', 'app', 'media', 'country', 'revenue_d1_read', 'revenue_d1_predicted', 'mape_day']])
    # 合并所有结果
    if len(results) == 0:
        print("No results to process")
        return None
    results_df = pd.concat(results, ignore_index=True)
    # 计算 mape_week
    results_df['mape_week'] = np.abs(results_df['revenue_d1_predicted'].sum() - results_df['revenue_d1_read'].sum()) / results_df['revenue_d1_read'].replace(0, np.nan).sum()
    
    # 添加 week 列
    results_df['week'] = results_df['install_day'].dt.strftime('%Y%W')
    # 处理无限值和 NaN
    results_df['mape_day'] = results_df['mape_day'].replace([np.inf, -np.inf], np.nan)

    # 确保列的顺序正确
    results_df = results_df[['install_day', 'app', 'media', 'country', 'revenue_d1_read', 'revenue_d1_predicted', 'mape_day', 'week', 'mape_week']]
    return results_df

def writeVerificationResultsToTable(df, dayStr):
    print('try to write verification results to table:')
    print(df.head(5))
    if 'o' in globals():
        table_name = 'lastwar_predict_revenue_day1_by_spend_verification'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s' % (dayStr), create_partition=True, arrow=True) as writer:
            # 将 install_day 转换为字符串
            df['install_day'] = df['install_day'].dt.strftime('%Y%m%d')
            writer.write(df)
        print(f"Verification results written to table partition day={dayStr}.")
    else:
        print('writeVerificationResultsToTable failed, o is not defined')
        print(dayStr)
        print(df)

def main(isMedia, isCountry):
    global dayStr

    # 找到上周的周一和周日
    current_monday = pd.to_datetime(dayStr, format='%Y%m%d') - pd.Timedelta(days=pd.to_datetime(dayStr, format='%Y%m%d').dayofweek)
    last_week_monday = current_monday - pd.Timedelta(weeks=1)
    install_day_start = last_week_monday.strftime('%Y%m%d')
    last_week_sunday = last_week_monday + pd.Timedelta(days=6)
    install_day_end = last_week_sunday.strftime('%Y%m%d')

    print('install_day_start:', install_day_start)
    print('install_day_end:', install_day_end)

    # 获取上周一的字符串表示，用于加载模型
    models_monday = last_week_monday.strftime('%Y%m%d')

    platformList = ['android', 'ios']

    verification_results = []

    for platform in platformList:
        app = appDict[platform]
        print(f"\nProcessing platform: {platform}, app: {app}")

        # 确定媒体和国家的列表
        mediaList = ['ALL']
        countryList = ['ALL']

        if isMedia or isCountry:
            # 从模型表中获取媒体和国家列表
            media_condition = "" if isMedia else "and media = 'ALL'"
            country_condition = "" if isCountry else "and country = 'ALL'"

            sql_models = f'''
            select distinct
                media,
                country
            from
                lastwar_predict_revenue_day1_by_spend
            where
                day = '{models_monday}'
                and app = '{app}'
                {media_condition}
                {country_condition}
            '''
            print(sql_models)
            models_df = execSql(sql_models)

            if isMedia:
                mediaList = models_df['media'].unique().tolist()
                mediaList = [media if media else 'ALL' for media in mediaList]
                # mediaList 排除 ALL
                mediaList = [media for media in mediaList if media != 'ALL']

            if isCountry:
                countryList = models_df['country'].unique().tolist()
                countryList = [country if country else 'ALL' for country in countryList]
                # countryList 排除 ALL
                countryList = [country for country in countryList if country != 'ALL']

            # 确保列表中没有空值
            mediaList = [media for media in mediaList if media]
            countryList = [country for country in countryList if country]

        for media in mediaList:
            for country in countryList:
                print(f"\nProcessing media: {media}, country: {country}")

                # 获取实际数据
                actual_data = getActualData(install_day_start, install_day_end, platform, media, country)
                if actual_data.empty:
                    print(f"No actual data for media: {media}, country: {country}")
                    continue

                print(f"Actual data for media: {media}, country: {country}")
                print(actual_data.head(10))

                # 加载模型
                model = loadModels(models_monday, app, media, country)
                if model is None:
                    print(f"No models found for app: {app}, media: {media}, country: {country}")
                    continue

                # 进行预测并计算误差
                results_df = makePredictionsAndCalculateErrors(actual_data, model, app, media, country)
                if results_df is not None:
                    verification_results.append(results_df)
                else:
                    print(f"No verification results for media: {media}, country: {country}")

    # 合并所有结果并写入表格
    if verification_results:
        final_results_df = pd.concat(verification_results, ignore_index=True)
        # 写入表格
        writeVerificationResultsToTable(final_results_df, dayStr)
    else:
        print("No verification results to write.")

if __name__ == "__main__":
    init()
    createTable()
    deletePartition(dayStr)
    # 依次调用 main 函数
    main(False, False)
    main(True, False)
    main(False, True)
    main(True, True)
