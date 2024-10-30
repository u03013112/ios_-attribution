import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
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

def getHistoricalData(payUserGroupList, force=False, install_day_start=20240401, install_day_end=20241025):
    filename = '/src/data/lastwar_historical_data_20240401_20241025.csv'
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
    :return: 处理后的 DataFrame，适用于 Prophet 模型。
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

    # 4. 删除原有的 revenue_1d 和 pu_1d 列
    columns_to_drop = ['revenue_1d', 'pu_1d']
    aggregated_data = aggregated_data.drop(columns=[col for col in columns_to_drop if col in aggregated_data.columns], errors='ignore')

    # 5. 重塑数据，从宽格式转换为长格式
    revenue_cols = [f"revenue_1d_{group['name']}" for group in payUserGroupList]
    pu_cols = [f"pu_1d_{group['name']}" for group in payUserGroupList]

    # 使用 pd.melt 将 revenue 列转为长格式
    revenue_melted = aggregated_data.melt(
        id_vars=['install_day'],
        value_vars=revenue_cols,
        var_name='pay_user_group',
        value_name='revenue_1d'
    )

    # 提取 pay_user_group_name，从 'revenue_1d_<name>' 中提取 '<name>'
    revenue_melted['pay_user_group_name'] = revenue_melted['pay_user_group'].str.replace('revenue_1d_', '', regex=False)

    # 使用 pd.melt 将 pu 列转为长格式
    pu_melted = aggregated_data.melt(
        id_vars=['install_day'],
        value_vars=pu_cols,
        var_name='pay_user_group_pu',
        value_name='pu_1d'
    )

    # 提取 pay_user_group_name，从 'pu_1d_<name>' 中提取 '<name>'
    pu_melted['pay_user_group_name'] = pu_melted['pay_user_group_pu'].str.replace('pu_1d_', '', regex=False)

    # 合并 revenue 和 pu 数据
    merged_long = pd.merge(
        revenue_melted[['install_day', 'pay_user_group_name', 'revenue_1d']],
        pu_melted[['install_day', 'pay_user_group_name', 'pu_1d']],
        on=['install_day', 'pay_user_group_name'],
        how='inner'
    )

    # 6. 添加其他聚合列，如 'cost'
    other_cols = ['cost']
    final_df = pd.merge(
        merged_long,
        aggregated_data[['install_day'] + other_cols],
        on='install_day',
        how='left'
    )

    # 7. 创建最终的数据框，适应 Prophet 模型的要求
    df = pd.DataFrame({
        'ds': final_df['install_day'],
        'cost': final_df['cost'],
        'y': final_df['revenue_1d'],
        'pud1': final_df['pu_1d'],
        'pay_user_group_name': final_df['pay_user_group_name'],
    })

    # 8. 按日期排序
    df = df.sort_values('ds', ascending=True)

    # 9. 移除含NaN的行
    df = df.dropna()

    return df

def train(train_df):    
    # 创建和训练Prophet模型
    model = Prophet()
    model.add_regressor('cost')
    model.fit(train_df)

    # 打印模型训练日志
    print("Model Training Completed")

    return model

def predict(model, future_df):
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
    return np.abs((actual - predicted) / (actual + 1)) * 100  # 加1以避免除以0

def main(group_by_media=False, group_by_country=False):
    """
    主函数，实现按 pay_user_group_name 单独训练和预测，并计算误差。

    :param group_by_media: 是否按媒体分组，默认为False
    :param group_by_country: 是否按国家分组，默认为False
    """
    payUserGroupList = [
        {'name': 'all', 'min': 0, 'max': 999999999},
        # 如需多个支付分组，请取消以下注释并根据需要调整
        # {'name': '0_1', 'min': 0, 'max': 1},
        # {'name': '1_2', 'min': 1, 'max': 2},
        # {'name': '2_3', 'min': 2, 'max': 3},
        # {'name': '3_inf', 'min': 3, 'max': np.inf},
    ]

    # 获取历史数据
    historical_data = getHistoricalData(payUserGroupList, force=False)

    # 获取所有媒体和国家的列表
    if group_by_media:
        mediaList = ['Facebook Ads', 'applovin_int', 'googleadwords_int']
    else:
        mediaList = [None]

    if group_by_country:
        countryList = ['GCC', 'JP', 'KR', 'T1', 'T2', 'T3', 'TW', 'US']
    else:
        countryList = [None]

    # 初始化结果列表
    results = []

    # 遍历每个媒体和国家组合
    for media in mediaList:
        for country in countryList:
            group_name = f"media:{media}" if media else "media:all"
            group_name += f"_country:{country}" if country else "_country:all"
            print(f"\n正在处理分组: {group_name}")

            # 数据预处理
            df = preprocessData(historical_data, payUserGroupList, media, country)

            # 获取所有 pay_user_group_name 的唯一值
            pay_user_group_names = df['pay_user_group_name'].unique()

            # 定义预测的日期范围
            test_start_date = '2024-08-05'
            test_end_date = '2024-10-13'

            # 转换为日期格式
            current_date = pd.to_datetime(test_start_date)
            end_date = pd.to_datetime(test_end_date)

            # 遍历每个 pay_user_group_name
            for group_name_single in pay_user_group_names:
                print(f"  处理 pay_user_group_name: {group_name_single}")

                # 过滤当前组的数据
                group_df = df[df['pay_user_group_name'] == group_name_single].copy()

                # 初始化预测日期
                temp_current_date = current_date

                while temp_current_date <= end_date:
                    # 定义训练集为过去60天
                    train_start_date = temp_current_date - timedelta(days=60)
                    train_end_date = temp_current_date - timedelta(days=1)
                    train_subset = group_df[(group_df['ds'] >= train_start_date) & (group_df['ds'] <= train_end_date)]

                    if len(train_subset) < 30:
                        print(f"    训练数据不足（少于30条），跳过日期: {temp_current_date.date()}")
                        temp_current_date += timedelta(days=7)
                        continue

                    # 训练模型
                    model = train(train_subset)

                    # 定义未来7天的预测集
                    future_dates = pd.date_range(start=temp_current_date, periods=7)
                    future_df = group_df[group_df['ds'].isin(future_dates)][['ds', 'cost']].copy()

                    if not future_df.empty:
                        # 预测
                        predictions = predict(model, future_df)
                        predictions['country'] = country if country else 'all'
                        predictions['media'] = media if media else 'all'
                        predictions['pay_user_group_name'] = group_name_single

                        # 获取实际的 y 值
                        actuals = group_df[group_df['ds'].isin(future_dates)][['ds', 'y']].copy()
                        actuals = actuals.rename(columns={'y': 'actual_y'})

                        # 合并预测结果与测试数据
                        merged = pd.merge(predictions, actuals, on='ds', how='left')

                        # 计算MAPE
                        merged['mape'] = calculate_mape(merged['actual_y'], merged['yhat'])

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

    # 1. 按 pay_user_group_name 和天计算误差统计
    pay_group_day_mape = results_df.groupby(['pay_user_group_name', 'ds']).agg(
        actual_y=pd.NamedAgg(column='actual_y', aggfunc='sum'),
        yhat=pd.NamedAgg(column='yhat', aggfunc='sum'),
        mape=pd.NamedAgg(column='mape', aggfunc='mean')
    ).reset_index()

    # 保存到 CSV
    pay_group_day_mape_filename = '/src/data/mape_by_pay_user_group_by_day.csv'
    pay_group_day_mape.to_csv(pay_group_day_mape_filename, index=False)
    print(f"\n按 pay_user_group_name 和天统计的MAPE已保存到 {pay_group_day_mape_filename}")

    # 2. 按 pay_user_group_name 和周计算误差统计
    results_df['week'] = results_df['ds'].dt.isocalendar().week
    pay_group_week_mape = results_df.groupby(['pay_user_group_name', 'week']).agg(
        actual_y=pd.NamedAgg(column='actual_y', aggfunc='sum'),
        yhat=pd.NamedAgg(column='yhat', aggfunc='sum'),
        mape=pd.NamedAgg(column='mape', aggfunc='mean')
    ).reset_index()

    # 保存到 CSV
    pay_group_week_mape_filename = '/src/data/mape_by_pay_user_group_by_week.csv'
    pay_group_week_mape.to_csv(pay_group_week_mape_filename, index=False)
    print(f"按 pay_user_group_name 和周统计的MAPE已保存到 {pay_group_week_mape_filename}")

    # 3. 按安装日期（天）计算误差统计
    daily_mape = results_df.groupby('ds').agg(
        actual_y=pd.NamedAgg(column='actual_y', aggfunc='sum'),
        yhat=pd.NamedAgg(column='yhat', aggfunc='sum'),
        mape=pd.NamedAgg(column='mape', aggfunc='mean')
    ).reset_index()

    # 保存到 CSV
    daily_mape_filename = '/src/data/mape_by_day.csv'
    daily_mape.to_csv(daily_mape_filename, index=False)
    print(f"按天统计的MAPE已保存到 {daily_mape_filename}")

    # 4. 按周统计 MAPE
    weekly_mape = results_df.groupby('week').agg(
        actual_y=pd.NamedAgg(column='actual_y', aggfunc='sum'),
        yhat=pd.NamedAgg(column='yhat', aggfunc='sum'),
        mape=pd.NamedAgg(column='mape', aggfunc='mean')
    ).reset_index()

    # 保存到 CSV
    weekly_mape_filename = '/src/data/mape_by_week.csv'
    weekly_mape.to_csv(weekly_mape_filename, index=False)
    print(f"按周统计的MAPE已保存到 {weekly_mape_filename}")

    # 5. 按国家和媒体分组后统计MAPE的天平均和周平均
    # 确保 'week' 列已存在
    if 'week' not in results_df.columns:
        results_df['week'] = results_df['ds'].dt.isocalendar().week

    # 按国家和媒体分组，计算天平均MAPE
    country_media_daily_mape = results_df.groupby(['country', 'media', 'ds']).agg(
        daily_mape=pd.NamedAgg(column='mape', aggfunc='mean')
    ).reset_index()

    # 计算天平均MAPE
    country_media_daily_avg_mape = country_media_daily_mape.groupby(['country', 'media']).agg(
        avg_daily_mape=pd.NamedAgg(column='daily_mape', aggfunc='mean')
    ).reset_index()

    # 按国家和媒体分组，计算周平均MAPE
    country_media_weekly_mape = results_df.groupby(['country', 'media', 'week']).agg(
        weekly_mape=pd.NamedAgg(column='mape', aggfunc='mean')
    ).reset_index()

    # 计算周平均MAPE
    country_media_weekly_avg_mape = country_media_weekly_mape.groupby(['country', 'media']).agg(
        avg_weekly_mape=pd.NamedAgg(column='weekly_mape', aggfunc='mean')
    ).reset_index()

    # 打印结果到终端
    print("\n按国家和媒体分组的天平均MAPE:")
    print(country_media_daily_avg_mape)

    print("\n按国家和媒体分组的周平均MAPE:")
    print(country_media_weekly_avg_mape)

    # 6. 保存按国家和媒体分组的MAPE统计到 CSV
    country_media_daily_avg_mape_filename = '/src/data/mape_by_country_media_daily_avg.csv'
    country_media_daily_avg_mape.to_csv(country_media_daily_avg_mape_filename, index=False)
    print(f"\n按国家和媒体分组的天平均MAPE已保存到 {country_media_daily_avg_mape_filename}")

    country_media_weekly_avg_mape_filename = '/src/data/mape_by_country_media_weekly_avg.csv'
    country_media_weekly_avg_mape.to_csv(country_media_weekly_avg_mape_filename, index=False)
    print(f"按国家和媒体分组的周平均MAPE已保存到 {country_media_weekly_avg_mape_filename}")

    # 7. 保存所有结果到 CSV 文件
    # 建议保存 `results_df` 以便进一步分析
    all_results_filename = '/src/data/all_predictions_and_errors.csv'
    results_df.to_csv(all_results_filename, index=False)
    print(f"\n所有预测结果及误差已保存到 {all_results_filename}")

    # 8. 计算并输出每组的MAPE的平均值
    overall_average_mape = results_df['mape'].mean()
    print(f"\n整体模型的MAPE的平均值: {overall_average_mape:.2f}%")

    # 额外计算按付费分组的平均 MAPE 并打印
    pay_group_average_mape = pay_group_day_mape['mape'].mean()
    print(f"按 pay_user_group_name 分组的整体平均 MAPE: {pay_group_average_mape:.2f}%")

if __name__ == '__main__':
    main()
    # 例如，如需按媒体和国家分组进行，可以调用：
    # main(group_by_media=True, group_by_country=True)
