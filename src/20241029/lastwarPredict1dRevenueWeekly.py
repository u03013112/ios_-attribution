import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json

import sys
sys.path.append('/src')
from src.maxCompute import execSql

payUserGroupList = [
    {'name': '0_1', 'min': 0, 'max': 1},
    {'name': '1_2', 'min': 1, 'max': 2},
    {'name': '2_3', 'min': 2, 'max': 3},
    {'name': '3_inf', 'min': 3, 'max': np.inf},
]

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
select
    install_day,
    CASE 
        WHEN mediasource = 'bytedanceglobal_int' THEN 'tiktokglobal_int' 
        ELSE mediasource 
    END AS mediasource,
    country,
    sum(usd) as usd
from
    tmp_lw_cost_and_roi_by_day
where
    install_day between {install_day_start} and {install_day_end}
group by
    install_day,
    mediasource,
    country
;

@d1_purchase_data :=
SELECT
    install_day,
    game_uid,
    country,
    mediasource,
    sum(revenue_value_usd) as revenue_1d
FROM
    dwd_overseas_revenue_allproject
WHERE
    app = 502
    AND app_package = 'com.fun.lastwar.gp'
    AND zone = 0
    AND day > {install_day_start}
    AND install_day between {install_day_start} and {install_day_end}
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
select
    d1.game_uid,
    d1.install_day,
    d1.country,
    d1.mediasource,
    d1.revenue_1d,
    map.countrygroup as countrygroup
from
    @d1_purchase_data as d1
    left join cdm_laswwar_country_map as map on d1.country = map.country
;

@result :=
SELECT
    install_day,
    countrygroup as country,
    mediasource,{revenue_case}
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

select
    r.install_day,
    r.country,
    r.mediasource,{', '.join([f"r.revenue_1d_{group['name']}" for group in payUserGroupList])},
    {', '.join([f"r.pu_1d_{group['name']}" for group in payUserGroupList])},
    r.revenue_1d,
    r.pu_1d,
    c.usd as cost
from
    @result as r
    left join @cost_data as c on r.install_day = c.install_day
    and r.country = c.country
    and r.mediasource = c.mediasource;
        '''
        print(sql)
        data = execSql(sql)
        data.to_csv(filename, index=False)

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
    # 假设数据包含以下列：
    # 'usd', 'revenue_1d_0_1', 'revenue_1d_1_2', 'revenue_1d_2_3', 'revenue_1d_3_inf',
    # 'pu_1d_0_1', 'pu_1d_1_2', 'pu_1d_2_3', 'pu_1d_3_inf', 'ins', 'pud1'
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
    # 假设这些列名为 'revenue_1d' 和 'pu_1d'
    columns_to_drop = ['revenue_1d', 'pu_1d']
    aggregated_data = aggregated_data.drop(columns=[col for col in columns_to_drop if col in aggregated_data.columns], errors='ignore')

    # 5. 重塑数据，从宽格式转换为长格式
    # 首先，定义 revenue 和 pu 的列
    revenue_cols = [f"revenue_1d_{group['name']}" for group in payUserGroupList]
    pu_cols = [f"pu_1d_{group['name']}" for group in payUserGroupList]

    # 使用 pd.melt 将 revenue 列转为长格式
    revenue_melted = aggregated_data.melt(
        id_vars=['install_day'],
        value_vars=revenue_cols,
        var_name='pay_user_group',
        value_name='revenue_1d'
    )

    # 提取 pay_user_group_name，从 'revenue_1d_0_1' 中提取 '0_1'
    revenue_melted['pay_user_group_name'] = revenue_melted['pay_user_group'].str.replace('revenue_1d_', '')

    # 使用 pd.melt 将 pu 列转为长格式
    pu_melted = aggregated_data.melt(
        id_vars=['install_day'],
        value_vars=pu_cols,
        var_name='pay_user_group_pu',
        value_name='pu_1d'
    )

    # 提取 pay_user_group_name，从 'pu_1d_0_1' 中提取 '0_1'
    pu_melted['pay_user_group_name'] = pu_melted['pay_user_group_pu'].str.replace('pu_1d_', '')

    # 合并 revenue 和 pu 数据
    merged_long = pd.merge(
        revenue_melted[['install_day', 'pay_user_group_name', 'revenue_1d']],
        pu_melted[['install_day', 'pay_user_group_name', 'pu_1d']],
        on=['install_day', 'pay_user_group_name']
    )

    # 6. 添加其他聚合列，如 'usd', 'ins', 'pud1'
    # 因为这些列在 aggregated_data 中已经按 install_day 聚合，可以与 merged_long 合并
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
    model.add_regressor('ad_spend')
    model.add_regressor('is_weekend')
    model.fit(train_df)

    # 打印模型训练日志
    print("Model Training Completed")

    return model

def predict(model, future_df):
    # 调用模型进行预测
    forecast = model.predict(future_df)
    return forecast[['ds', 'yhat']]


if __name__ == '__main__':
    historical_data = getHistoricalData(payUserGroupList)

    # print(historical_data.head())
    df = preprocessData(historical_data, payUserGroupList)
    
    print(df.head(20))