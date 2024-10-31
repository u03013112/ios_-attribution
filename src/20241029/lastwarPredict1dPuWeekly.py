import os
import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import timedelta

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def generate_case_statements(group_list, value_field, aggregate='SUM', is_count=False):
    """
    生成SQL的CASE语句部分，用于按组聚合字段。

    :param group_list: 组列表，每个组是一个字典，包含'name', 'min', 'max'
    :param value_field: 要聚合的字段名
    :param aggregate: 聚合函数，默认为'SUM'，如果is_count为True则使用'SUM'
    :param is_count: 是否为计数操作
    :return: 生成的SQL片段字符串
    """
    statements = []
    for group in group_list:
        # 处理Infinity的情况
        max_value = '999999999' if group['max'] == np.inf else group['max']
        if is_count:
            statement = f"""
        SUM(
            CASE
                WHEN {value_field} > {group['min']}
                AND {value_field} <= {max_value} THEN 1
                ELSE 0
            END
        ) AS pu_1d_{group['name']},
            """
        else:
            statement = f"""
        SUM(
            CASE
                WHEN {value_field} > {group['min']}
                AND {value_field} <= {max_value} THEN {value_field}
                ELSE 0
            END
        ) AS revenue_1d_{group['name']},
            """
        statements.append(statement)
    return "\n".join(statements)

def getHistoricalData(payUserGroupList, prefix, force=False, install_day_start=20240401, install_day_end=20241025):
    """
    获取历史数据，如果缓存存在且不强制更新，则直接读取缓存，否则执行SQL查询并缓存结果。

    :param payUserGroupList: 用户支付组列表，每个组包含'name', 'min', 'max'。
    :param prefix: 文件前缀，用于区分不同的组配置。
    :param force: 是否强制重新查询数据，默认为False。
    :param install_day_start: 数据起始日期。
    :param install_day_end: 数据结束日期。
    :return: 获取到的历史数据 DataFrame。
    """
    filename = f'/src/data/{prefix}_historical_data_{install_day_start}_{install_day_end}.csv'
    if os.path.exists(filename) and not force:
        data = pd.read_csv(filename)
    else:
        # 生成动态的CASE语句
        revenue_case = generate_case_statements(payUserGroupList, 'revenue_1d', aggregate='SUM', is_count=False)
        count_case = generate_case_statements(payUserGroupList, 'revenue_1d', aggregate='SUM', is_count=True)

        sql = f'''
@cost_data :=
SELECT
    install_day,
    CASE 
        WHEN mediasource = 'bytedanceglobal_int' THEN 'tiktokglobal_int' 
        ELSE mediasource 
    END AS mediasource,
    country,
    SUM(usd) AS usd
FROM
    tmp_lw_cost_and_roi_by_day
WHERE
    install_day BETWEEN {install_day_start} AND {install_day_end}
GROUP BY
    install_day,
    CASE 
        WHEN mediasource = 'bytedanceglobal_int' THEN 'tiktokglobal_int' 
        ELSE mediasource 
    END,
    country
;

@d1_purchase_data :=
SELECT
    install_day,
    game_uid,
    country,
    mediasource,
    SUM(revenue_value_usd) AS revenue_1d
FROM
    dwd_overseas_revenue_allproject
WHERE
    app = 502
    AND app_package = 'com.fun.lastwar.gp'
    AND zone = 0
    AND day > {install_day_start}
    AND install_day BETWEEN {install_day_start} AND {install_day_end}
    AND DATEDIFF(
        FROM_UNIXTIME(event_time),
        FROM_UNIXTIME(CAST(install_timestamp AS BIGINT)),
        'dd'
    ) = 0
GROUP BY
    install_day,
    game_uid,
    country,
    mediasource
;

@country_map :=
SELECT
    d1.game_uid,
    d1.install_day,
    d1.country,
    d1.mediasource,
    d1.revenue_1d,
    map.countrygroup AS countrygroup
FROM
    @d1_purchase_data AS d1
    LEFT JOIN cdm_laswwar_country_map AS map 
        ON d1.country = map.country
;

@result :=
SELECT
    install_day,
    countrygroup AS country,
    mediasource,
    {revenue_case}
    {count_case}
    SUM(revenue_1d) AS revenue_1d,
    COUNT(DISTINCT game_uid) AS pu_1d
FROM
    @country_map
GROUP BY
    install_day,
    countrygroup,
    mediasource
;

SELECT
    r.install_day,
    r.country,
    r.mediasource,
    {', '.join([f"r.revenue_1d_{group['name']}" for group in payUserGroupList])},
    {', '.join([f"r.pu_1d_{group['name']}" for group in payUserGroupList])},
    r.revenue_1d,
    r.pu_1d,
    c.usd AS cost
FROM
    @result AS r
    LEFT JOIN @cost_data AS c 
        ON r.install_day = c.install_day
        AND r.country = c.country
        AND r.mediasource = c.mediasource
;
        '''
        print("执行的SQL语句如下：\n")
        print(sql)
        data = execSql(sql)
        data.to_csv(filename, index=False)
        print(f"历史数据已保存到 {filename}")

    return data

def preprocessData(data, payUserGroupList, media=None, country=None):
    """
    预处理数据，包括日期转换、过滤、聚合、重塑和特征工程。
    
    :param data: 输入的 DataFrame，包含聚合后的数据。
    :param payUserGroupList: 用户支付组列表，每个组包含'name', 'min', 'max'。
    :param media: 可选参数，用于过滤特定的媒体来源。
    :param country: 可选参数，用于过滤特定的国家。
    :return: 处理后的 DataFrame，适用于 Prophet 模型，并包含 ARPPU 信息
    """
    
    # 1. 转换 'install_day' 列为日期格式
    data['install_day'] = pd.to_datetime(data['install_day'], format='%Y%m%d')
    
    # 2. 过滤数据
    if media:
        data = data[data['mediasource'] == media]
    if country:
        data = data[data['country'] == country]
    
    # 3. 按 'install_day' 分组并汇总所需列
    aggregation_dict = {
        'cost': 'sum',
    }
    
    # 动态添加所有 revenue_1d_* 和 pu_1d_* 列的聚合方式
    for group in payUserGroupList:
        revenue_col = f"revenue_1d_{group['name']}"
        pu_col = f"pu_1d_{group['name']}"
        aggregation_dict[revenue_col] = 'sum'
        aggregation_dict[pu_col] = 'sum'
    
    aggregated_data = data.groupby('install_day').agg(aggregation_dict).reset_index()
    
    # 4. 重塑数据，从宽格式转换为长格式
    # 构建所有相关列的列表
    revenue_cols = [f"revenue_1d_{group['name']}" for group in payUserGroupList]
    pu_cols = [f"pu_1d_{group['name']}" for group in payUserGroupList]
    
    # 使用 pd.melt 将 revenue 和 pu 列转为长格式
    melted_revenue = aggregated_data.melt(
        id_vars=['install_day', 'cost'],
        value_vars=revenue_cols,
        var_name='pay_user_group',
        value_name='revenue_1d'
    )
    
    melted_pu = aggregated_data.melt(
        id_vars=['install_day'],
        value_vars=pu_cols,
        var_name='pay_user_group',
        value_name='pu_1d'
    )
    
    # 提取 pay_user_group_name
    melted_revenue['pay_user_group_name'] = melted_revenue['pay_user_group'].str.replace('revenue_1d_', '', regex=False)
    melted_pu['pay_user_group_name'] = melted_pu['pay_user_group'].str.replace('pu_1d_', '', regex=False)
    
    # 合并 revenue 和 pu 数据
    merged_long = pd.merge(
        melted_revenue[['install_day', 'cost', 'pay_user_group_name', 'revenue_1d']],
        melted_pu[['install_day', 'pay_user_group_name', 'pu_1d']],
        on=['install_day', 'pay_user_group_name'],
        how='left'
    )
    
    # 5. 计算 actual_ARPPU 和 predicted_ARPPU
    # 计算实际 ARPPU
    merged_long['actual_ARPPU'] = merged_long['revenue_1d'] / merged_long['pu_1d']
    merged_long['actual_ARPPU'].replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # 计算预测 ARPPU：先shift一天，再计算过去56天的均值
    # 这里假设数据按 'install_day' 升序排列
    merged_long = merged_long.sort_values(['pay_user_group_name', 'install_day'])
    merged_long['predicted_ARPPU'] = merged_long.groupby('pay_user_group_name')['actual_ARPPU'].shift(1).rolling(window=56, min_periods=56).mean()
    
    # 6. 重命名和选择最终列
    merged_long = merged_long.rename(columns={'install_day': 'ds', 'pu_1d': 'y'})
    
    # 最终选择列
    df = merged_long[['ds', 'cost', 'y', 'pay_user_group_name', 'actual_ARPPU', 'predicted_ARPPU', 'revenue_1d']]
    
    return df

def train_model(train_df):
    """
    训练 Prophet 模型。

    :param train_df: 训练数据的 DataFrame。
    :return: 训练好的 Prophet 模型。
    """
    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('cost')
    model.fit(train_df)

    # 打印模型训练日志
    print("Model Training Completed")

    return model

def predict(model, future_df):
    """
    使用训练好的模型进行预测。

    :param model: 训练好的 Prophet 模型。
    :param future_df: 未来数据的 DataFrame，包含需要预测的日期及回归变量。
    :return: 预测结果的 DataFrame，包含 'ds' 和 'yhat' 列。
    """
    # 调用模型进行预测
    forecast = model.predict(future_df)
    return forecast[['ds', 'yhat']]

def calculate_mape(actual, predicted):
    """
    计算平均绝对百分比误差（MAPE）。

    :param actual: 实际值
    :param predicted: 预测值
    :return: MAPE值
    """
    return (np.abs((actual - predicted) / actual) * 100).replace([np.inf, -np.inf], np.nan)  # 避免除以0并处理inf

def main(payUserGroupList, prefix, group_by_media=False, group_by_country=False):
    """
    主函数，实现按 pay_user_group_name 单独训练和预测，并计算误差。
    
    :param payUserGroupList: 用户支付组列表，每个组包含'name', 'min', 'max'。
    :param prefix: 文件名前缀，用于区分不同的支付用户分组配置。
    :param group_by_media: 是否按媒体分组，默认为False
    :param group_by_country: 是否按国家分组，默认为False
    """
    # 获取历史数据
    historical_data = getHistoricalData(payUserGroupList, prefix, force=False)

    # 获取所有媒体和国家的列表
    if group_by_media:
        mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int']
    else:
        mediaList = [None]

    if group_by_country:
        countryList = ['JP', 'KR', 'US', 'T1']
    else:
        countryList = [None]

    # 初始化结果列表
    results = []

    # 定义预测的日期范围（根据用户要求修改）
    test_start_date = '2024-08-05'
    test_end_date = '2024-10-13'

    # 转换为日期格式
    current_date = pd.to_datetime(test_start_date)
    end_date = pd.to_datetime(test_end_date)

    # 遍历每个媒体和国家组合
    for media in mediaList:
        for country in countryList:
            group_context_name = f"media:{media}" if media else "media:all"
            group_context_name += f"_country:{country}" if country else "_country:all"
            print(f"  处理分组: {group_context_name}")

            # 预处理数据，获取单一 DataFrame
            df = preprocessData(historical_data, payUserGroupList, media=media, country=country)

            # 获取所有 pay_user_group_name 的唯一值
            pay_group_names = df['pay_user_group_name'].unique()

            # 遍历每个 pay_user_group_name
            for group_name_single in pay_group_names:
                print(f"\n正在处理 pay_user_group_name: {group_name_single}")

                # 过滤当前组的数据
                group_df = df[df['pay_user_group_name'] == group_name_single].copy()
                
                # 初始化预测日期
                temp_current_date = current_date

                while temp_current_date <= end_date:
                    # 定义训练集为过去60天
                    train_start_date = temp_current_date - timedelta(days=60)
                    train_end_date = temp_current_date - timedelta(days=1)
                    train_subset = group_df[(group_df['ds'] >= train_start_date) & (group_df['ds'] <= train_end_date)]

                    if train_subset.empty:
                        print(f"    训练数据为空，跳过日期: {temp_current_date.date()}")
                        temp_current_date += timedelta(days=7)
                        continue

                    if len(train_subset) < 30:
                        print(f"    训练数据不足（少于30条），跳过日期: {temp_current_date.date()}")
                        temp_current_date += timedelta(days=7)
                        continue

                    # 训练模型
                    model = train_model(train_subset[['ds', 'y', 'cost']])

                    # 定义未来7天的预测集
                    future_dates = pd.date_range(start=temp_current_date, periods=7)
                    
                    # 获取对应的 predicted_ARPPU
                    # predicted_ARPPU 是基于过去56天的均值，并在数据预处理阶段已经计算好
                    last_predicted_arppu = train_subset['predicted_ARPPU'].iloc[-1] if not train_subset['predicted_ARPPU'].isna().all() else np.nan

                    # 获取未来7天的成本，如果不存在则填充为0
                    # 假设成本数据在 historical_data 中，包括未来7天
                    future_cost = group_df[group_df['ds'].isin(future_dates)]['cost']
                    future_cost_values = future_cost.values
                    # 如果部分日期缺少成本数据，填充为0
                    if len(future_cost_values) < 7:
                        future_cost_values = list(future_cost_values) + [0] * (7 - len(future_cost_values))

                    future_df = pd.DataFrame({
                        'ds': future_dates,
                        'cost': future_cost_values
                    })

                    # 进行预测
                    predictions = predict(model, future_df)
                    predictions['country'] = country if country else 'all'
                    predictions['media'] = media if media else 'all'
                    predictions['pay_user_group_name'] = group_name_single

                    # 获取实际的 pu 值（如果可用）
                    actuals = group_df[group_df['ds'].isin(future_dates)][['ds','cost','y','actual_ARPPU','predicted_ARPPU','revenue_1d']].copy()

                    # 合并预测与实际数据
                    merged = pd.merge(predictions, actuals, on='ds', how='left', suffixes=('', '_actual'))

                    # # 填充缺失的 predicted_ARPPU
                    # merged['predicted_ARPPU'] = merged['predicted_ARPPU'].fillna(last_predicted_arppu)

                    # 计算预测的收入，使用 predicted_ARPPU
                    merged['predicted_revenue'] = merged['yhat'] * merged['predicted_ARPPU']

                    # 获取实际的收入（使用实际的 ARPPU）
                    merged['actual_revenue'] = merged['y'] * merged['actual_ARPPU']

                    # 计算 MAPE
                    merged['mape_pu'] = calculate_mape(merged['y'], merged['yhat'])
                    merged['mape_revenue'] = calculate_mape(merged['actual_revenue'], merged['predicted_revenue'])
                    merged['mape_arppu'] = calculate_mape(merged['actual_ARPPU'], merged['predicted_ARPPU'])

                    # 添加结果到列表
                    results.append(merged)

                    # 移动到下一周
                    temp_current_date += timedelta(days=7)

    # 将所有结果合并
    if results:
        results_df = pd.concat(results, ignore_index=True)
    else:
        print("没有任何预测结果生成。")
        return

    # ===========================
    # 详细的误差统计
    # ===========================

    print(results_df)

    # 1. 按 pay_user_group_name 和天计算误差统计
    print(f"\n[{prefix}] 按 pay_user_group_name 和天计算误差统计:")
    pay_group_day_mape = results_df.groupby(['pay_user_group_name', 'ds', 'media', 'country']).agg(
        cost=pd.NamedAgg(column='cost', aggfunc='sum'),
        actual_pu=pd.NamedAgg(column='y', aggfunc='sum'),
        yhat_pu=pd.NamedAgg(column='yhat', aggfunc='sum'),
        actual_revenue=pd.NamedAgg(column='actual_revenue', aggfunc='sum'),
        predicted_revenue=pd.NamedAgg(column='predicted_revenue', aggfunc='sum'),
        actual_arppu=pd.NamedAgg(column='actual_ARPPU', aggfunc='mean'),
        predicted_arppu=pd.NamedAgg(column='predicted_ARPPU', aggfunc='mean'),
    ).reset_index()
    pay_group_day_mape['mape_pu'] = calculate_mape(pay_group_day_mape['actual_pu'], pay_group_day_mape['yhat_pu'])
    pay_group_day_mape['mape_arppu'] = calculate_mape(pay_group_day_mape['actual_arppu'], pay_group_day_mape['predicted_arppu'])
    pay_group_day_mape['mape_revenue'] = calculate_mape(pay_group_day_mape['actual_revenue'], pay_group_day_mape['predicted_revenue'])
    
    # 保存到 CSV
    pay_group_day_mape_filename = f'/src/data/{prefix}_mape_by_pay_user_group_by_day.csv'
    pay_group_day_mape.to_csv(pay_group_day_mape_filename, index=False)
    print(f"按天结果已保存到 {pay_group_day_mape_filename}")

    # 计算并保存按 pay_user_group_name、country 和 media 的天平均 MAPE
    country_media_daily_avg_mape = pay_group_day_mape.groupby(['pay_user_group_name', 'country', 'media']).agg(
        avg_daily_mape_pu=pd.NamedAgg(column='mape_pu', aggfunc='mean'),
        avg_daily_mape_revenue=pd.NamedAgg(column='mape_revenue', aggfunc='mean'),
        avg_daily_mape_arppu=pd.NamedAgg(column='mape_arppu', aggfunc='mean')
    ).reset_index()
    # 保存到 CSV
    country_media_daily_avg_mape_filename = f'/src/data/{prefix}_mape_by_pay_user_group_country_media_daily_avg.csv'
    country_media_daily_avg_mape.to_csv(country_media_daily_avg_mape_filename, index=False)
    print(f"结果已保存到： {country_media_daily_avg_mape_filename}")

    # 打印按 pay_user_group_name 和国家和媒体分组的天平均MAPE
    print(f"\n[{prefix}] 按 pay_user_group_name 和国家和媒体分组的天平均MAPE:")
    print(country_media_daily_avg_mape)
    print("------------------------------------------------------\n")


    # 2. 按 pay_user_group_name 和周计算误差统计
    print(f"\n[{prefix}] 按 pay_user_group_name 和周计算误差统计:")
    # 将 'ds' 转换为周标识（例如，使用年-周格式）
    results_df['year'] = results_df['ds'].dt.isocalendar().year
    results_df['week'] = results_df['ds'].dt.isocalendar().week
    results_df['year_week'] = results_df['year'].astype(str) + '-' + results_df['week'].astype(str)

    # 按 pay_user_group_name 和 year_week 汇总 y 和 yhat
    pay_group_week_mape = results_df.groupby(['pay_user_group_name', 'year_week', 'country', 'media']).agg(
        actual_pu=pd.NamedAgg(column='y', aggfunc='sum'),
        yhat_pu=pd.NamedAgg(column='yhat', aggfunc='sum'),
        actual_revenue=pd.NamedAgg(column='actual_revenue', aggfunc='sum'),
        predicted_revenue=pd.NamedAgg(column='predicted_revenue', aggfunc='sum'),
        actual_arppu=pd.NamedAgg(column='actual_ARPPU', aggfunc='mean'),
        predicted_arppu=pd.NamedAgg(column='predicted_ARPPU', aggfunc='mean'),
    ).reset_index()

    # 计算周 MAPE
    pay_group_week_mape['mape_pu'] = calculate_mape(pay_group_week_mape['actual_pu'], pay_group_week_mape['yhat_pu'])
    pay_group_week_mape['mape_arppu'] = calculate_mape(pay_group_week_mape['actual_arppu'], pay_group_week_mape['predicted_arppu'])
    pay_group_week_mape['mape_revenue'] = calculate_mape(pay_group_week_mape['actual_revenue'], pay_group_week_mape['predicted_revenue'])

    # 保存到 CSV
    pay_group_week_mape_filename = f'/src/data/{prefix}_mape_by_pay_user_group_by_week.csv'
    pay_group_week_mape.to_csv(pay_group_week_mape_filename, index=False)
    print(f"按周结果已保存到  {pay_group_week_mape_filename}")

    # 计算并保存按 pay_user_group_name、country 和 media 的周平均 MAPE
    country_media_weekly_avg_mape = pay_group_week_mape.groupby(['pay_user_group_name', 'country', 'media']).agg(
        avg_weekly_mape_pu=pd.NamedAgg(column='mape_pu', aggfunc='mean'),
        avg_weekly_mape_revenue=pd.NamedAgg(column='mape_revenue', aggfunc='mean'),
        avg_weekly_mape_arppu=pd.NamedAgg(column='mape_arppu', aggfunc='mean')
    ).reset_index()
    # 保存到 CSV
    country_media_weekly_avg_mape_filename = f'/src/data/{prefix}_mape_by_pay_user_group_country_media_weekly_avg.csv'
    country_media_weekly_avg_mape.to_csv(country_media_weekly_avg_mape_filename, index=False)
    print(f"结果已保存到： {country_media_weekly_avg_mape_filename}")

    # 打印按 pay_user_group_name 和国家和媒体分组的周平均MAPE
    print(f"\n[{prefix}] 按 pay_user_group_name 和国家和媒体分组的周平均MAPE:")
    print(country_media_weekly_avg_mape)
    print("------------------------------------------------------\n")

    # 3. 按安装日期（天）计算误差统计
    print(f"\n[{prefix}] 按安装日期（天）计算误差统计:")
    install_day_mape = results_df.groupby(['ds', 'country', 'media']).agg(
        cost=pd.NamedAgg(column='cost', aggfunc='sum'),
        actual_pu=pd.NamedAgg(column='y', aggfunc='sum'),
        yhat_pu=pd.NamedAgg(column='yhat', aggfunc='sum'),
        actual_revenue=pd.NamedAgg(column='actual_revenue', aggfunc='sum'),
        predicted_revenue=pd.NamedAgg(column='predicted_revenue', aggfunc='sum'),
        actual_arppu=pd.NamedAgg(column='actual_ARPPU', aggfunc='mean'),
        predicted_arppu=pd.NamedAgg(column='predicted_ARPPU', aggfunc='mean'),
    ).reset_index()
    install_day_mape['mape_pu'] = calculate_mape(install_day_mape['actual_pu'], install_day_mape['yhat_pu'])
    install_day_mape['mape_arppu'] = calculate_mape(install_day_mape['actual_arppu'], install_day_mape['predicted_arppu'])
    install_day_mape['mape_revenue'] = calculate_mape(install_day_mape['actual_revenue'], install_day_mape['predicted_revenue'])

    # 保存到 CSV
    install_day_mape_filename = f'/src/data/{prefix}_mape_by_install_day.csv'
    install_day_mape.to_csv(install_day_mape_filename, index=False)
    print(f"按安装日期（天）结果已保存到 {install_day_mape_filename}")

    # 计算并保存按安装日期、country 和 media 的平均 MAPE
    country_media_install_day_avg_mape = install_day_mape.groupby(['country', 'media']).agg(
        avg_daily_mape_pu=pd.NamedAgg(column='mape_pu', aggfunc='mean'),
        avg_daily_mape_revenue=pd.NamedAgg(column='mape_revenue', aggfunc='mean'),
        avg_daily_mape_arppu=pd.NamedAgg(column='mape_arppu', aggfunc='mean')
    ).reset_index()
    # 保存到 CSV
    country_media_install_day_avg_mape_filename = f'/src/data/{prefix}_mape_by_install_day_country_media_avg.csv'
    country_media_install_day_avg_mape.to_csv(country_media_install_day_avg_mape_filename, index=False)
    print(f"结果已保存到： {country_media_install_day_avg_mape_filename}")

    # 打印按安装日期、country 和 media 的平均 MAPE
    print(f"\n[{prefix}] 按安装日期、country 和 media 的平均 MAPE:")
    print(country_media_install_day_avg_mape)
    print("------------------------------------------------------\n")

    # 4. 按安装日期（周）计算误差统计
    print(f"\n[{prefix}] 按安装日期（周）计算误差统计:")
    # 将 'ds' 转换为周标识（例如，使用年-周格式）
    results_df['year'] = results_df['ds'].dt.isocalendar().year
    results_df['week'] = results_df['ds'].dt.isocalendar().week
    results_df['year_week'] = results_df['year'].astype(str) + '-' + results_df['week'].astype(str)

    # 按 year_week 汇总 y 和 yhat
    install_week_mape = results_df.groupby(['year_week', 'country', 'media']).agg(
        actual_pu=pd.NamedAgg(column='y', aggfunc='sum'),
        yhat_pu=pd.NamedAgg(column='yhat', aggfunc='sum'),
        actual_revenue=pd.NamedAgg(column='actual_revenue', aggfunc='sum'),
        predicted_revenue=pd.NamedAgg(column='predicted_revenue', aggfunc='sum'),
        actual_arppu=pd.NamedAgg(column='actual_ARPPU', aggfunc='mean'),
        predicted_arppu=pd.NamedAgg(column='predicted_ARPPU', aggfunc='mean'),
    ).reset_index()

    # 计算周 MAPE
    install_week_mape['mape_pu'] = calculate_mape(install_week_mape['actual_pu'], install_week_mape['yhat_pu'])
    install_week_mape['mape_arppu'] = calculate_mape(install_week_mape['actual_arppu'], install_week_mape['predicted_arppu'])
    install_week_mape['mape_revenue'] = calculate_mape(install_week_mape['actual_revenue'], install_week_mape['predicted_revenue'])

    # 保存到 CSV
    install_week_mape_filename = f'/src/data/{prefix}_mape_by_install_week.csv'
    install_week_mape.to_csv(install_week_mape_filename, index=False)
    print(f"按安装日期（周）结果已保存到 {install_week_mape_filename}")

    # 计算并保存按安装日期、country 和 media 的周平均 MAPE
    country_media_install_week_avg_mape = install_week_mape.groupby(['country', 'media']).agg(
        avg_weekly_mape_pu=pd.NamedAgg(column='mape_pu', aggfunc='mean'),
        avg_weekly_mape_revenue=pd.NamedAgg(column='mape_revenue', aggfunc='mean'),
        avg_weekly_mape_arppu=pd.NamedAgg(column='mape_arppu', aggfunc='mean')
    ).reset_index()

    # 保存到 CSV
    country_media_install_week_avg_mape_filename = f'/src/data/{prefix}_mape_by_install_week_country_media_avg.csv'
    country_media_install_week_avg_mape.to_csv(country_media_install_week_avg_mape_filename, index=False)
    print(f"结果已保存到： {country_media_install_week_avg_mape_filename}")

    # 打印按安装日期、country 和 media 的周平均 MAPE
    print(f"\n[{prefix}] 按安装日期、country 和 media 的周平均 MAPE:")
    print(country_media_install_week_avg_mape)
    print("------------------------------------------------------\n")





if __name__ == '__main__':
    # 定义不同的 payUserGroupList 和对应的前缀
    configurations = [
        {
            'payUserGroupList': [
                {'name': 'all', 'min': 0, 'max': np.inf},
            ],
            'prefix': 'lw20241030_pu_all1',
            'group_by_media': False,
            'group_by_country': False
        },
        # {
        #     'payUserGroupList': [
        #         {'name': 'all', 'min': 0, 'max': np.inf},
        #     ],
        #     'prefix': 'lw20241030_pu_all1_media',
        #     'group_by_media': True,
        #     'group_by_country': False
        # },
        # {
        #     'payUserGroupList': [
        #         {'name': 'all', 'min': 0, 'max': np.inf},
        #     ],
        #     'prefix': 'lw20241030_pu_all1_country',
        #     'group_by_media': False,
        #     'group_by_country': True
        # },
        # {
        #     'payUserGroupList': [
        #         {'name': 'all', 'min': 0, 'max': np.inf},
        #     ],
        #     'prefix': 'lw20241030_pu_all1_media_country',
        #     'group_by_media': True,
        #     'group_by_country': True
        # },
        # {
        #     'payUserGroupList': [
        #         {'name': '0_2', 'min': 0, 'max': 2},
        #         {'name': '2_inf', 'min': 2, 'max': np.inf},
        #     ],
        #     'prefix': 'lw20241030_pu_two2',
        #     'group_by_media': False,
        #     'group_by_country': False
        # },
        # {
        #     'payUserGroupList': [
        #         {'name': '0_2', 'min': 0, 'max': 2},
        #         {'name': '2_inf', 'min': 2, 'max': np.inf},
        #     ],
        #     'prefix': 'lw20241030_pu_two2_media',
        #     'group_by_media': True,
        #     'group_by_country': False
        # },
        # {
        #     'payUserGroupList': [
        #         {'name': '0_2', 'min': 0, 'max': 2},
        #         {'name': '2_inf', 'min': 2, 'max': np.inf},
        #     ],
        #     'prefix': 'lw20241030_pu_two2_country',
        #     'group_by_media': False,
        #     'group_by_country': True
        # },
        # {
        #     'payUserGroupList': [
        #         {'name': '0_2', 'min': 0, 'max': 2},
        #         {'name': '2_inf', 'min': 2, 'max': np.inf},
        #     ],
        #     'prefix': 'lw20241030_pu_two2_media_country',
        #     'group_by_media': True,
        #     'group_by_country': True
        # },
    ]

    # 运行每个配置
    for config in configurations:
        print(f"##### 运行配置: {config['prefix']} #####")
        main(
            payUserGroupList=config['payUserGroupList'],
            prefix=config['prefix'],
            group_by_media=config.get('group_by_media', False),
            group_by_country=config.get('group_by_country', False)
        )
