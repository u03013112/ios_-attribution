import os
import sys
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_from_json

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
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local

        dayStr = '20241016'  # 本地测试时的日期，可自行修改

    # 定义 app 的字典
    appDict = {'android': 'com.fun.lastwar.gp', 'ios': 'id6448786147'}

def fetchModel(current_monday_str, app, media='ALL', country='ALL'):
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
    day = '{current_monday_str}'
    and app = '{app}'
    {media_condition}
    {country_condition}
;
        '''
    print("执行 SQL 获取模型：")
    print(sql)
    models_df = execSql(sql)
    if models_df.empty:
        print("未找到符合条件的模型。")
        return None
    # 取出第一个模型
    row = models_df.iloc[0]
    model_json = row['model']
    model = model_from_json(model_json)
    print("模型加载成功。")
    return model

def getPast4WeeksData(dayStr, platform, media='ALL', country='ALL'):
    end_date = pd.to_datetime(dayStr, format='%Y%m%d')
    current_monday = end_date - pd.Timedelta(days=end_date.weekday())
    last_sunday = current_monday - pd.Timedelta(days=1)
    start_date = last_sunday - pd.Timedelta(weeks=4) + pd.Timedelta(days=1)
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = last_sunday.strftime('%Y%m%d')

    table_name = 'tmp_lw_cost_and_roi_by_day' if platform == 'android' else 'tmp_lw_cost_and_roi_by_day_ios'

    # 媒体映射，从模型表的媒体名称映射为实际数据表中的媒体名称
    media_mapping = {
        'FACEBOOK': 'Facebook Ads',
        'GOOGLE': 'googleadwords_int',
        'APPLOVIN': 'applovin_int'
    }
    mapped_media = media_mapping.get(media, media)

    media_condition = f"and mediasource = '{mapped_media}'" if media != 'ALL' else ""
    country_condition = f"and country = '{country}'" if country != 'ALL' else ""

    sql = f'''
select
    install_day,
    sum(usd) as usd
from
    {table_name}
where
    install_day between '{start_date_str}' and '{end_date_str}'
    {media_condition}
    {country_condition}
group by
    install_day
;
        '''
    print("执行 SQL 获取过去4周的数据：")
    print(sql)
    data = execSql(sql)
    if data.empty:
        print("未获取到过去4周的数据。")
        return None

    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')
    data.set_index('install_day', inplace=True)
    return data

def calculateDailySpendProportions(past_spend_data):
    past_spend_data = past_spend_data.copy()
    past_spend_data['week'] = past_spend_data.index.isocalendar().week
    past_spend_data['weekday'] = past_spend_data.index.weekday  # 周一=0，周日=6

    weekly_spend = past_spend_data.groupby(['week']).agg({'usd': 'sum'}).rename(columns={'usd': 'usd_week_total'})
    past_spend_data = past_spend_data.merge(weekly_spend, on='week', left_index=True)

    past_spend_data['daily_proportion'] = past_spend_data['usd'] / past_spend_data['usd_week_total']

    daily_proportions = past_spend_data.groupby('weekday').agg({'daily_proportion': 'mean'}).reset_index()

    # 确保比例之和为1
    total_proportion = daily_proportions['daily_proportion'].sum()
    daily_proportions['daily_proportion'] /= total_proportion

    print("过去4周的平均每日花费比例：")
    print(daily_proportions)
    return daily_proportions

def calculateSpendBaseline(past_spend_data):
    past_spend_data['week'] = past_spend_data.index.isocalendar().week
    # weekly_totals = past_spend_data.groupby('week').agg({'usd': 'sum'})
    # baseline = weekly_totals['usd'].mean()

    # 找到最后一周的花费总额
    last_week = past_spend_data['week'].max()
    baseline = past_spend_data[past_spend_data['week'] == last_week]['usd'].sum()

    return baseline

def generateSpendLevels(baseline, daily_proportions):
    adjustment_levels = [-0.2, -0.15, -0.1, 0, 0.1, 0.15, 0.2]
    spend_levels = []
    for adj in adjustment_levels:
        adjusted_baseline = baseline * (1 + adj)
        spend_level = {'adjustment': adj, 'total_spend': adjusted_baseline}
        daily_spends = daily_proportions.copy()
        daily_spends['expected_spend'] = daily_spends['daily_proportion'] * adjusted_baseline
        daily_spends = daily_spends.sort_values('weekday').reset_index(drop=True)
        spend_level['daily_spends'] = daily_spends[['weekday', 'expected_spend']]
        spend_levels.append(spend_level)
    return spend_levels

def predictRevenueForLevel(spend_level, model, current_week_monday):
    daily_spends = spend_level['daily_spends']

    current_week_dates = [current_week_monday + pd.Timedelta(days=i) for i in range(7)]
    weekdays = [d.weekday() for d in current_week_dates]
    date_df = pd.DataFrame({'install_day': current_week_dates, 'weekday': weekdays})

    prediction_df = date_df.merge(daily_spends, on='weekday', how='left')
    prediction_df.rename(columns={'expected_spend': 'ad_spend'}, inplace=True)
    prediction_df['ds'] = prediction_df['install_day']

    forecast = model.predict(prediction_df[['ds', 'ad_spend']])

    prediction_df['predicted_revenue'] = forecast['yhat']
    total_revenue = prediction_df['predicted_revenue'].sum()
    total_spend = spend_level['total_spend']

    # 计算 ROI 为收益率，即（收入 - 花费）/ 花费
    roi = (total_revenue) / total_spend if total_spend != 0 else np.nan
    spend_level['predicted_revenue'] = total_revenue
    spend_level['roi'] = roi  # 例如，1.5 表示 ROI 为 1.5

    # 创建新的 DataFrame，不对原有 DataFrame 进行修改
    daily_predictions = prediction_df[['install_day', 'weekday', 'ad_spend', 'predicted_revenue']].copy()
    daily_predictions.sort_values('install_day', inplace=True)

    spend_level['daily_predictions'] = daily_predictions
    return

def findBestLevel(levels, roi_threshold):
    levels_sorted = sorted(levels, key=lambda x: x['adjustment'], reverse=True)
    for level in levels_sorted:
        if level['roi'] >= roi_threshold:
            return level
    return None  # 没有找到满足条件的级别

def getRoiThreshold(lastSundayStr, app, media, country):
    media_condition = f"and media = '{media}'" if media != 'ALL' else "and media = 'ALL'"
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
    print("执行 SQL 获取 ROI 阈值：")
    print(sql)
    roi_threshold_df = execSql(sql)
    if roi_threshold_df.empty:
        print("未找到 ROI 阈值。用保守值 2% 代替。")
        return 0.02
    return roi_threshold_df.iloc[0]['roi_001_best']

def main(group_by_media=False, group_by_country=False):
    global dayStr

    current_date = pd.to_datetime(dayStr, format='%Y%m%d')
    current_week_monday = current_date - pd.Timedelta(days=current_date.weekday())
    current_monday_str = current_week_monday.strftime('%Y%m%d')

    lastSunday = current_week_monday - pd.Timedelta(days=1)
    lastSundayStr = lastSunday.strftime('%Y%m%d')

    platformList = ['android', 'ios']
    appDict = {'android': 'com.fun.lastwar.gp', 'ios': 'id6448786147'}

    for platform in platformList:

        if platform == 'ios' and group_by_media == True:
            print('ios 平台不支持按媒体分组')
            continue

        app = appDict[platform]
        mediaList = ['ALL']
        countryList = ['ALL']

        if group_by_media or group_by_country:
            # 从模型表中获取媒体和国家列表
            media_condition = "" if group_by_media else "and media = 'ALL'"
            country_condition = "" if group_by_country else "and country = 'ALL'"

            sql_models = f'''
            select distinct
                media,
                country
            from
                lastwar_predict_revenue_day1_by_spend
            where
                day = '{current_monday_str}'
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

        for media in mediaList:
            for country in countryList:
                print(f"\n处理平台：{platform}，媒体：{media}，国家：{country}")
                # 第1步：获取本周一的模型
                model = fetchModel(current_monday_str, app, media, country)
                if model is None:
                    print("获取模型失败，跳过此组合。")
                    continue

                # 第2步：获取过去4周的数据
                past_spend_data = getPast4WeeksData(dayStr, platform, media, country)
                if past_spend_data is None:
                    print("获取过去4周数据失败，跳过此组合。")
                    continue

                # 第3步：计算每日花费比例
                daily_proportions = calculateDailySpendProportions(past_spend_data)

                # 第4步：计算花费基线
                baseline = calculateSpendBaseline(past_spend_data)
                print(f"花费基线：{baseline:.2f}")

                # 第5步和第6步：生成花费档位并计算每日预计花费
                spend_levels = generateSpendLevels(baseline, daily_proportions)

                # 第7步和第8步：使用模型预测每个档位的收入和计算ROI
                for level in spend_levels:
                    predictRevenueForLevel(level, model, current_week_monday)
                
                print("各档位的预测结果：")
                for level in spend_levels:
                    adj_percent = level['adjustment'] * 100
                    roi_value = level['roi']
                    print(f"调整 {adj_percent:.0f}%：预测ROI = {roi_value*100:.2f}%")

                # 第9步：获取 ROI 阈值
                roi_threshold = getRoiThreshold(lastSundayStr, app, media, country)
                
                print(f"ROI 阈值：{roi_threshold*100:.2f}%")
                # 保守起见，将 ROI 阈值 升高一些
                # 鉴于之前经验的误差范围，将阈值提高 10%
                roi_threshold *= 1.1
                print(f"保守的 ROI 阈值：{roi_threshold*100:.2f}%")

                # 找到 ROI 大于等于阈值的最高档位并打印结果
                best_level = findBestLevel(spend_levels, roi_threshold)
                if best_level:
                    adjustment_percent = best_level['adjustment'] * 100
                    print(f"最佳调整档位：{adjustment_percent:.0f}%")
                    print(f"总花费：{best_level['total_spend']:.2f}")
                    print(f"预测总收入：{best_level['predicted_revenue']:.2f}")
                    print(f"预测ROI：{best_level['roi']*100:.2f}%")
                    print("每日预计花费和预测收入：")
                    daily_preds = best_level['daily_predictions']
                    daily_preds['install_day'] = daily_preds['install_day'].dt.strftime('%Y-%m-%d')
                    daily_preds['ad_spend'] = daily_preds['ad_spend'].round(2)
                    daily_preds['predicted_revenue'] = daily_preds['predicted_revenue'].round(2)
                    print(daily_preds[['install_day', 'ad_spend', 'predicted_revenue']].to_string(index=False))
                else:
                    print(f"没有档位的预测ROI能达到或超过 {roi_threshold*100:.2f}%。")
                    print("各档位的预测ROI如下：")
                    for level in sorted(spend_levels, key=lambda x: x['adjustment']):
                        adj_percent = level['adjustment'] * 100
                        roi_value = level['roi']
                        print(f"调整 {adj_percent:.0f}%：预测ROI = {roi_value*100:.2f}%")

if __name__ == "__main__":
    init()
    main(False, False)
    main(True, False)
    main(False, True)
    main(True, True)
