# 新方案整理
# 先计算整体服务器的季节性，然后再将这个季节性作为额外特征计算每个服务器的季节性
# 最后将所有的季节性都排除掉，找到趋势
# 并将趋势、和预测 画图


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score
from prophet.diagnostics import cross_validation, performance_metrics

import os
import json


import sys
sys.path.append('/src')

from src.lastwar.ss.ss import ssSql

def getData(endday='2025-02-25'):
    filename = f'/src/data/e2_data_{endday}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        print(f'load file {filename}')
    else:
        sql = f'''
WITH event_data AS (
    SELECT
        lw_cross_zone,
        DATE(
            IF(
                "#zone_offset" IS NOT NULL
                AND "#zone_offset" BETWEEN -30
                AND 30,
                DATE_ADD(
                    'second',
                    CAST((0 - "#zone_offset") * 3600 AS INTEGER),
                    "#event_time"
                ),
                "#event_time"
            )
        ) AS event_date,
        usd,
        "#user_id"
    FROM
        v_event_3
    WHERE
        "$part_event" = 's_pay_new'
        AND "$part_date" BETWEEN '2024-10-01'
        AND '{endday}'
),
user_data AS (
    SELECT
        "#user_id"
    FROM
        v_user_3
    WHERE
        "lwu_is_gm" IS NOT NULL
)
SELECT
    e.event_date as day,
    e.lw_cross_zone as server_id,
    ROUND(
        SUM(
            e.usd
        ),
        4
    ) AS revenue
FROM
    event_data e
    LEFT JOIN user_data u ON e."#user_id" = u."#user_id"
WHERE
    e.event_date BETWEEN DATE '2024-10-01'
    AND DATE '{endday}'
    AND e.lw_cross_zone IN ('APS3','APS4','APS5','APS6','APS7','APS8','APS9','APS10','APS11','APS12','APS13','APS14','APS15','APS16','APS17','APS18','APS19','APS20','APS21','APS22','APS23','APS24','APS25','APS26','APS27','APS28','APS29','APS30','APS31','APS32','APS33','APS34','APS35','APS36')
    AND u."#user_id" IS NULL
GROUP BY
    e.lw_cross_zone,
    e.event_date
ORDER BY
    revenue DESC;
        '''

        lines = ssSql(sql=sql)

        # print('lines:',len(lines))
        # print(lines[:10])

        data = []
        for line in lines:
            if line == '':
                continue
            j = json.loads(line)
            data.append(j)
        df = pd.DataFrame(data,columns=["day","server_id","revenue"])

        # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
        df['day'] = pd.to_datetime(df['day'], errors='coerce')
        df = df.dropna(subset=['day'])

        # 将server_id转换为整数
        def convert_server_id(server_id):
            try:
                return int(server_id[3:])
            except:
                return np.nan

        df = df[df['server_id'] != '(null)']        
        df['server_id_int'] = df['server_id'].apply(convert_server_id)
        df = df.dropna(subset=['server_id_int'])

        # 将无法转换为浮点数的字符串替换为 NaN，然后再用 0 替换 NaN
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)

        df.to_csv(filename, index=False)

    return df

df = getData('2025-04-08')
df = df[df['day'] >= '2024-10-16']

df0 = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]
df0 = df0[['day', 'revenue', 'server_id_int']]
df0 = df0.rename(columns={'day': 'ds', 'revenue': 'y', 'server_id_int': 'server_id'})
df0['cap'] = 1e5
df0['floor'] = 0

# 由于目前数据不足，暂时设计使用56天预测56天的方式
# 找到4个时间点：昨日，昨日-56天，昨日-56*2天，昨日-56*3天
# 从昨日-56*3~昨日-56*2天作为训练集
# 从昨日-56*2天~昨日-56天作为验证集
# 从昨日-56天~昨日作为测试集
yesterday = pd.to_datetime(df0['ds'].max())
train_start = yesterday - pd.Timedelta(days=56 * 3)
train_end = yesterday - pd.Timedelta(days=56 * 2)
valid_start = yesterday - pd.Timedelta(days=56 * 2)
valid_end = yesterday - pd.Timedelta(days=56)
test_start = yesterday - pd.Timedelta(days=56)
test_end = yesterday

train_start_str = train_start.strftime('%Y-%m-%d')
train_end_str = train_end.strftime('%Y-%m-%d')
valid_start_str = valid_start.strftime('%Y-%m-%d')
valid_end_str = valid_end.strftime('%Y-%m-%d')
test_start_str = test_start.strftime('%Y-%m-%d')
test_end_str = test_end.strftime('%Y-%m-%d')

print(f'train_start: {train_start_str}, train_end: {train_end_str}')
print(f'valid_start: {valid_start_str}, valid_end: {valid_end_str}')
print(f'test_start: {test_start_str}, test_end: {test_end_str}')

# 汇总数据
aggregated_data = df0.groupby('ds').agg({'y': 'mean'}).reset_index()
aggregated_data['cap'] = 1e5
aggregated_data['floor'] = 0

agg_train_data = aggregated_data[
    (aggregated_data['ds'] >= train_start_str) & (aggregated_data['ds'] <= train_end_str)
]
agg_valid_data = aggregated_data[
    (aggregated_data['ds'] >= valid_start_str) & (aggregated_data['ds'] <= valid_end_str)
]
agg_test_data = aggregated_data[
    (aggregated_data['ds'] >= test_start_str) & (aggregated_data['ds'] <= test_end_str)
]

# 优化全局模型参数并训练模型
changepoint_prior_scales = [0.1, 0.5, 1.0]
seasonality_prior_scales = [0.03, 0.1]
best_params_agg = None
best_mape_agg = float('inf')
best_model_agg = None

for cps in changepoint_prior_scales:
    for sps in seasonality_prior_scales:
        model = Prophet(
            growth='logistic',
            daily_seasonality=True, 
            weekly_seasonality=True, 
            yearly_seasonality=True,
            changepoint_prior_scale=cps,
            seasonality_prior_scale=sps
        )
        model.fit(agg_train_data)
        
        # 验证模型
        forecast_valid = model.predict(agg_valid_data)
        mape_valid = mean_absolute_percentage_error(agg_valid_data['y'], forecast_valid['yhat'])

        if mape_valid < best_mape_agg:
            best_mape_agg = mape_valid
            best_params_agg = {'changepoint_prior_scale': cps, 'seasonality_prior_scale': sps}
            best_model_agg = model

# 用最优参数重新训练模型
best_model_agg2 = Prophet(
    growth='logistic',
    daily_seasonality=True, 
    weekly_seasonality=True, 
    yearly_seasonality=True,
    changepoint_prior_scale=best_params_agg['changepoint_prior_scale'],
    seasonality_prior_scale=best_params_agg['seasonality_prior_scale']
)
best_model_agg2.fit(agg_valid_data)

# 预测测试集
forecast_test = best_model_agg2.predict(agg_test_data)
mape_test = mean_absolute_percentage_error(agg_test_data['y'], forecast_test['yhat'])

# 保存最佳参数和测试集MAPE到CSV
results = pd.DataFrame([{
    'best_changepoint_prior_scale': best_params_agg['changepoint_prior_scale'],
    'best_seasonality_prior_scale': best_params_agg['seasonality_prior_scale'],
    'mape_test': mape_test
}])
results.to_csv('/src/data/e3_agg.csv', index=False)
print(f'Best parameters: {best_params_agg}')
print(f'Test MAPE: {mape_test}')


# 画图，一张大图中上下两张小图
# 上面图，使用best_model_agg预测训练集和验证集
agg_train_valid_data = pd.concat([agg_train_data, agg_valid_data])
agg_train_valid_data = agg_train_valid_data.sort_values(by='ds')
forecast_train_valid = best_model_agg.predict(agg_train_valid_data)

plt.figure(figsize=(14, 10))

plt.subplot(2, 1, 1)
# agg_train_valid_data['ds'] = pd.to_datetime(agg_train_valid_data['ds'])
plt.plot(agg_train_valid_data['ds'], agg_train_valid_data['y'], label='Actual')
plt.plot(agg_train_valid_data['ds'], forecast_train_valid['yhat'], label='Predicted')
plt.fill_between(agg_train_valid_data['ds'], forecast_train_valid['yhat_lower'], forecast_train_valid['yhat_upper'], color='gray', alpha=0.2)
plt.axvline(x=valid_start_str, color='gray', linestyle='--')
plt.title('Training + Validation Set')
plt.xlabel('Date')
plt.ylabel('Revenue')
plt.legend()

# 下面图，使用best_model_agg2预测验证集与测试集
agg_valid_test_data = pd.concat([agg_valid_data, agg_test_data])
agg_valid_test_data = agg_valid_test_data.sort_values(by='ds')
forecast_valid_test = best_model_agg2.predict(agg_valid_test_data)

plt.subplot(2, 1, 2)
# agg_train_valid_data = agg_train_valid_data.sort_values(by='ds')
plt.plot(agg_valid_test_data['ds'], agg_valid_test_data['y'], label='Actual')
plt.plot(agg_valid_test_data['ds'], forecast_valid_test['yhat'], label='Predicted')
plt.fill_between(agg_valid_test_data['ds'], forecast_valid_test['yhat_lower'], forecast_valid_test['yhat_upper'], color='gray', alpha=0.2)
plt.axvline(x=test_start_str, color='gray', linestyle='--')
plt.title('Validation + Test Set')
plt.xlabel('Date')
plt.ylabel('Revenue')
plt.legend()

plt.tight_layout()
plt.savefig('/src/data/e3_agg.png')
plt.show()
# x是ds，y是 y 和 yhat
# yhat_lower 和 yhat_upper 中间填充灰色
# 训练集与测试集中间画一条竖着的虚线，用于区分训练集和测试集

# 下面图，使用best_model_agg2预测验证集与测试集
# x是ds，y是 y 和 yhat
# yhat_lower 和 yhat_upper 中间填充灰色
# 测验证集与测试集中间画一条竖着的虚线，用于区分训练集和测试集
