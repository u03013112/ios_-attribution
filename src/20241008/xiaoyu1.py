import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getHistoricalData():
    filename = '/src/data/xiaoyu_historical_data_20240401_20241007.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
        select
            install_day,
            sum(usd) as usd,
            sum(d1) as d1,
            sum(ins) as ins,
            sum(pud1) as pud1
        from
            tmp_lw_cost_and_roi_by_day
        where
            install_day between 20240401
            and 20241007
        group by
            install_day;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)
        
    return data

def getHistoricalDataIOS():
    filename = '/src/data/xiaoyu_historical_data_ios_20240401_20241007.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = '''
select
    install_day,
    sum(usd) as usd,
    sum(d1) as d1,
    sum(ins) as ins,
    sum(pud1) as pud1
from
    tmp_lw_cost_and_roi_by_day_ios
where
    install_day between 20240401
    and 20241007
group by
    install_day;
        '''
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def train(train_df, model_path='prophet_model.json'):
    # 准备Prophet所需的数据格式
    prophet_train_df = train_df[['date', 'pud1_pct', 'ad_spend_pct', 'is_weekend']].copy()
    prophet_train_df.columns = ['ds', 'y', 'ad_spend_pct', 'is_weekend']

    prophet_train_df['cap'] = 1
    # 移除含NaN的行
    prophet_train_df = prophet_train_df.dropna()

    # 创建和训练Prophet模型
    model = Prophet(growth='logistic')
    model.add_regressor('ad_spend_pct')
    model.add_regressor('is_weekend')
    model.fit(prophet_train_df)

    # 保存模型
    with open(model_path, 'w') as f:
        f.write(model_to_json(model))

    return model

def predict(test_df, model):
    results = []

    # 进行成本增幅预测
    for index, row in test_df.iterrows():
        # 获取当前日期和相应数据
        current_date = row['date']
        current_ad_spend = row['ad_spend']
        
        # 预测日期是当前日期的下一天
        predict_date = current_date + pd.Timedelta(days=1)
        
        # 获取当前索引在 test_df 中的位置
        loc = test_df.index.get_loc(index)
        
        # 计算真实的花费变化比例
        if loc < len(test_df) - 1:
            next_day_row = test_df.iloc[loc + 1]
            cost_increase = (next_day_row['ad_spend'] - current_ad_spend) / current_ad_spend
            
            # 获取该日期的周末特征
            is_weekend_value = next_day_row['is_weekend']

            # 预测今日付费人数增幅
            future_df = pd.DataFrame({
                'ds': [predict_date],
                'ad_spend_pct': [cost_increase],
                'is_weekend': [is_weekend_value]
            })
            
            future_df['cap'] = 1

            # 调用模型进行预测
            forecast = model.predict(future_df)
            
            # 计算预估pud1
            predicted_pud1 = row['pud1'] * (1 + forecast['yhat'].values[0])

            # 计算预估收入
            predicted_revenue = row['arppu_daily_mean'] * predicted_pud1

            results.append({
                'date': predict_date,
                'weekday': predict_date.weekday(),
                'lastday_real_spend': current_ad_spend,
                'arppu_daily_mean': row['arppu_daily_mean'],
                'lastday_pud1': row['pud1'],
                'predictedpud1_pct': forecast['yhat'].values[0],
                'predictedpud1_pct_lower': forecast['yhat_lower'].values[0],
                'predictedpud1_pct_upper': forecast['yhat_upper'].values[0],
                'predictedpud1_pct_is_outlier': forecast['yhat'].values[0] < forecast['yhat_lower'].values[0] or forecast['yhat'].values[0] > forecast['yhat_upper'].values[0],
                'predicted_spend': next_day_row['ad_spend'],
                'predicted_pud1': predicted_pud1,
                'predicted_revenue': predicted_revenue,
                'cost_increase': cost_increase
            })

    # 转换为DataFrame
    results_df = pd.DataFrame(results)

    # 计算ARPPU的MAPE
    results_df['arppu_real'] = test_df['revenue'].values[1:] / test_df['pud1'].values[1:]
    results_df['arppu_predict'] = results_df['predicted_revenue'] / results_df['predicted_pud1']
    results_df['arppu_mape'] = np.abs((results_df['arppu_real'] - results_df['arppu_predict']) / results_df['arppu_real']) * 100

    # 计算付费用户MAPE
    results_df['pud1_read'] = test_df['pud1'].values[1:]
    results_df.rename(columns={'predicted_pud1':'pud1_predict'},inplace=True)
    results_df['pud1_mape'] = np.abs((results_df['pud1_predict'] - test_df['pud1'].values[1:]) / test_df['pud1'].values[1:]) * 100

    # 计算MAPE
    results_df['mape'] = np.abs((results_df['predicted_revenue'] - test_df['revenue'].values[1:]) / test_df['revenue'].values[1:]) * 100

    return results_df

# 
def predict2(test_df, model):
    results = []

    # 进行成本增幅预测
    for index, row in test_df.iterrows():
        # 获取当前日期和相应数据
        current_date = row['date']
        current_ad_spend = row['ad_spend']
        
        # 预测日期是当前日期的下一天
        predict_date = current_date + pd.Timedelta(days=1)
        
        # 获取当前索引在 test_df 中的位置
        loc = test_df.index.get_loc(index)
        
        # 计算真实的花费变化比例
        if loc < len(test_df) - 1:
            next_day_row = test_df.iloc[loc + 1]
            real_cost_increase = (next_day_row['ad_spend'] - current_ad_spend) / current_ad_spend
            
            # 获取该日期的周末特征
            is_weekend_value = 1 if predict_date.weekday() in [5, 6] else 0  # 计算预测日期是否为周末

            # 生成可能的 cost_increase 数组
            possible_cost_increases = np.arange(real_cost_increase - 0.15, real_cost_increase + 0.15, 0.01)
            
            best_forecast = None
            best_cost_increase = None
            min_diff = float('inf')

            for cost_increase in possible_cost_increases:
                # 预测今日付费人数增幅
                future_df = pd.DataFrame({
                    'ds': [predict_date],
                    'ad_spend_pct': [cost_increase],
                    'is_weekend': [is_weekend_value]
                })
                
                # 调用模型进行预测
                forecast = model.predict(future_df)
                
                # 计算预估pud1
                predicted_pud1 = row['pud1'] * (1 + forecast['yhat'].values[0])

                # 计算预估收入
                predicted_revenue = row['arppu_daily_mean'] * predicted_pud1

                # 计算预测收入与真实收入的差值
                diff = abs(predicted_revenue - next_day_row['revenue'])

                if diff < min_diff:
                    min_diff = diff
                    best_forecast = forecast
                    best_cost_increase = cost_increase

            # 计算预估pud1
            predicted_pud1 = row['pud1'] * (1 + best_forecast['yhat'].values[0])

            # 计算预估收入
            predicted_revenue = row['arppu_daily_mean'] * predicted_pud1

            # 计算预测的花费
            predicted_spend = current_ad_spend * (1 + best_cost_increase)

            results.append({
                'date': predict_date,
                'weekday': predict_date.weekday(),
                'lastday_real_spend': current_ad_spend,
                'arppu_daily_mean': row['arppu_daily_mean'],
                'lastday_pud1': row['pud1'],
                'predictedpud1_pct': best_forecast['yhat'].values[0],
                'predicted_spend': predicted_spend,
                'predicted_pud1': predicted_pud1,
                'predicted_revenue': predicted_revenue,
                'cost_increase': best_cost_increase
            })

    # 转换为DataFrame
    results_df = pd.DataFrame(results)

    # 计算MAPE
    results_df['mape'] = np.abs((results_df['predicted_spend'] - test_df['ad_spend'].values[1:]) / test_df['ad_spend'].values[1:]) * 100

    return results_df


def predictOneDay(model,ds,ad_spend_pct,is_weekend):
    # 预测今日付费人数增幅
    future_df = pd.DataFrame({
        'ds': [ds],
        'ad_spend_pct': [ad_spend_pct],
        'is_weekend': [is_weekend]
    })

    # 调用模型进行预测
    forecast = model.predict(future_df)

    predictedpud1_pct = forecast['yhat'].values[0]
    return predictedpud1_pct

def arppu(N):
    historical_data = getHistoricalData()
    historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')
    aggregated_data = historical_data.groupby('install_day').agg({
        'usd': 'sum',
        'd1': 'sum',
        'ins': 'sum',
        'pud1': 'sum',
    }).reset_index()
    df = pd.DataFrame({
        'date': aggregated_data['install_day'],
        'ad_spend': aggregated_data['usd'],
        'revenue': aggregated_data['d1'], 
        'ins': aggregated_data['ins'],
        'pud1': aggregated_data['pud1'],
    })
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date', ascending=True)

    df['arppu'] = df['revenue'] / df['pud1']
    # 计算最近N天的ARPPU平均值，包括计算当日
    df['arppu_daily_mean'] = df['arppu'].shift(1).rolling(window=N).mean()
    df = df.dropna(subset=['arppu_daily_mean'])

    df['arppu_real'] = df['arppu']
    df['arppu_predict'] = df['arppu_daily_mean']
    df['arppu mape'] = np.abs((df['arppu_real'] - df['arppu_predict']) / df['arppu_real']) * 100

    test_df = df[(df['date'] >= '2024-09-13') & (df['date'] <= '2024-10-07')]
    # test_df = df[(df['date'] >= '2024-07-01') & (df['date'] <= '2024-09-12')]

    # print(test_df)
    mape = test_df['arppu mape'].mean()
    print(f"ARPPU MAPE: {mape:.2f}%")
    return mape



def main():
    # 设定N天的时间窗口
    N = 15  # 例如过去30天

    # 获取历史数据
    historical_data = getHistoricalData()
    # historical_data = getHistoricalDataIOS()

    # 转换 'install_day' 列为日期格式
    historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

    # 按 'install_day' 分组并汇总所需列
    aggregated_data = historical_data.groupby('install_day').agg({
        'usd': 'sum',
        'd1': 'sum',
        'ins': 'sum',
        'pud1': 'sum',
    }).reset_index()

    # 创建数据框
    df = pd.DataFrame({
        'date': aggregated_data['install_day'],
        'ad_spend': aggregated_data['usd'],
        'revenue': aggregated_data['d1'], 
        'ins': aggregated_data['ins'],
        'pud1': aggregated_data['pud1'],
    })

    # 确保日期列是日期格式
    df['date'] = pd.to_datetime(df['date'])
    # 按日期排序
    df = df.sort_values('date', ascending=True)

    # 添加周末特征
    df['is_weekend'] = df['date'].dt.dayofweek.isin([5, 6]).astype(int)

    # 计算花费百分比变化
    df['ad_spend_pct'] = df['ad_spend'].pct_change()
    df['arppu'] = df['revenue'] / df['pud1']
    df['pud1_pct'] = df['pud1'].pct_change()

    # 计算最近N天的ARPPU平均值，包括计算当日
    df['arppu_daily_mean'] = df['arppu'].shift(1).rolling(window=N, min_periods=1).mean()

    # 分割训练集和测试集
    # train_df = df[(df['date'] >= '2024-04-01') & (df['date'] <= '2024-09-12')]
    train_df = df[(df['date'] >= '2024-06-01') & (df['date'] <= '2024-09-12')]
    test_df = df[(df['date'] >= '2024-09-13') & (df['date'] <= '2024-10-07')]

    model_path = '/src/data/prophet_model_6.json'

    # # 检查模型是否存在
    # if os.path.exists(model_path):
    #     # 加载模型
    #     with open(model_path, 'r') as f:
    #         model = model_from_json(f.read())
    # else:
    #     # 训练模型
    #     model = train(train_df, model_path)
    
    model = train(train_df, model_path)

    # 进行预测
    results_df = predict(test_df, model)

    # 输出查询表单
    print("Results DataFrame:")
    print(results_df[['date','weekday', 'arppu_mape','pud1_mape','mape']])
    # results_df[['date', 'arppu_mape','pud1_mape','mape']].to_csv('/src/data/prediction_results_1.csv', index=False)

    # 输出到 CSV 文件
    results_df.to_csv('/src/data/prediction_results.csv', index=False)

    print(f"Results DataFrame has been saved")

    # 计算并输出ARPPU的MAPE的平均值
    average_arppu_mape = results_df['arppu_mape'].mean()
    print(f"所有ARPPU的MAPE的平均值: {average_arppu_mape:.2f}%")

    # 计算并输出付费用户数MAPE的平均值
    average_pud1_mape = results_df['pud1_mape'].mean()
    print(f"所有付费用户数MAPE的平均值: {average_pud1_mape:.2f}%")

    # 计算并输出所有MAPE的平均值
    average_mape = results_df['mape'].mean()
    print(f"所有MAPE的平均值: {average_mape:.2f}%")

# ?测试相同的ds，不同的ad_spend_pct结论是否具有一定的趋势
# ?结论：ad_spend_pct越大，predictedpud1_pct越大，虽然不是线性相关，但是有一定的趋势
# ?所以可以尝试反向推测，即按照不同比例的ad_spend_pct，计算预测ROI，再根据距离最近的ROI，找到ad_spend_pct，再与真实的ad_spend_pct进行比较
def test01():

    ds = pd.to_datetime('2024-09-25')
    is_weekend = 0

    model_path = '/src/data/prophet_model.json'
    with open(model_path, 'r') as f:
        model = model_from_json(f.read())

        for ad_spend_pct in [-0.3,0.2,0.1,0,0.1,0.2,0.3]:
            predictedpud1_pct = predictOneDay(model,ds,ad_spend_pct,is_weekend)
            print(f"{ad_spend_pct} predictedpud1_pct: {predictedpud1_pct}")


def arppuTest():
    minN = 1
    minMape = 100
    for N in range(1,31):
        print(f"N={N}")
        mape = arppu(N)
        if mape < minMape:
            minN = N
            minMape = mape
    print('-----------------')
    print(f"minN={minN},minMape={minMape}")

if __name__ == "__main__":
    main()
    # test01()
    # arppu(30)
    # arppuTest()
