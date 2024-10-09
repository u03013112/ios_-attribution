import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib.pyplot as plt

# 设定N天的时间窗口
N = 30  # 例如过去30天

# 从Excel文件中读取数据（此步骤可以替换为DW的代码）
historical_data = pd.read_excel('/Users/admin/Desktop/P2-ROI和花费的相关性/tmp_lw_cost_and_roi_by_day.xlsx')

# 转换 'install_day' 列为日期格式
historical_data['install_day'] = pd.to_datetime(historical_data['install_day'], format='%Y%m%d')

# 过滤日期范围
filtered_data = historical_data[
    (historical_data['install_day'] >= '2024-04-01') & (historical_data['install_day'] <= '2024-09-22')
#     & (~historical_data['country'].isin(['T2'])) # 排除T2和GCC
]

# 按 'install_day' 分组并汇总所需列
aggregated_data = filtered_data.groupby('install_day').agg({
    'usd': 'sum',
    'd7': 'sum',
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

# 添加周末特征
df['is_weekend'] = df['date'].dt.dayofweek.isin([5, 6]).astype(int)

# 计算花费百分比变化
df['ad_spend_pct'] = df['ad_spend'].pct_change()
df['arppu'] = df['revenue'] / df['pud1']
df['pud1_pct'] = df['pud1'].pct_change()

# 计算最近N天的ARPPU平均值，包括计算当日
df['arppu_daily_mean'] = df['arppu'].rolling(window=N, min_periods=1).mean()

# pud1_pct预测
# 准备Prophet所需的数据格式
prophet_df = df[['date', 'pud1_pct', 'ad_spend_pct', 'is_weekend']].copy()
prophet_df.columns = ['ds', 'y', 'ad_spend_pct', 'is_weekend']

# 移除含NaN的行
prophet_df = prophet_df.dropna()

# 创建和训练Prophet模型
model = Prophet()
model.add_regressor('ad_spend_pct')
model.add_regressor('is_weekend')
model.fit(prophet_df)

# 根据星期几设定不同的P和Q值--海外安卓
def get_pq_by_weekday(weekday):
    # 周一到周五 (0-4) 可以设定一组值
    if weekday == 0:  # 周一
        P = -0.3
        Q = 0
    elif weekday in [1, 2, 3]:  # 周二-周四
        P = -0.2
        Q = 0.2
    elif weekday == 4:  # 周五
        P = -0.1
        Q = 0.2
    elif weekday == 5:  # 周六
        P = 0
        Q = 0.4   
    elif weekday == 6:  # 周日
        P = -0.1
        Q = 0.4         
    return P, Q
    
# 根据星期几设定不同的P和Q值--海外IOS
def get_pq_by_weekday(weekday):
    # 周一到周五 (0-4) 可以设定一组值
    if weekday == 0:  # 周一
        P = -0.25
        Q = -0.05
    elif weekday == 1:  # 周二
        P = -0.2
        Q = 0.15
    elif weekday == 2:  # 周三
        P = -0.1
        Q = 0.05
    elif weekday == 3:  # 周四
        P = -0.1
        Q = 0.1
    elif weekday == 4:  # 周五
        P = -0.1
        Q = 0.15     
    elif weekday == 5:  # 周六
        P = -0.1
        Q = 0.4 
    elif weekday == 6:  # 周日
        P = -0.1
        Q = 0.15
    return P, Q

def generate_cost_increase_range(current_ad_spend, P, Q, step=10000):
    # 计算下限和上限的具体金额
    min_spend = current_ad_spend * (1 + P)
    max_spend = current_ad_spend * (1 + Q)

    # 生成以10000为步长的具体分段
    spend_steps = np.arange(current_ad_spend - 300000, current_ad_spend + 300001, step)

    # 确保上下限在范围中
    spend_steps = np.append(spend_steps, [min_spend, max_spend])

    # 排序并去重
    spend_steps = np.unique(np.sort(spend_steps))

    # 过滤范围只在下限和上限之间的值
    filtered_spend_steps = spend_steps[(spend_steps >= min_spend) & (spend_steps <= max_spend)]
    
    # 转换为相对于当前花费的增幅百分比
    cost_increase_range = (filtered_spend_steps / current_ad_spend) - 1

    # 添加 half 的增幅
    additional_increases = np.array([ P / 2, Q / 2])
    cost_increase_range = np.append(cost_increase_range, additional_increases)

    # 返回唯一的增幅范围
    return np.unique(cost_increase_range)


# 创建结果表单
results = []

# 从目标ROI表中获取数据
target_roi_data = pd.read_excel('/Users/admin/Desktop/P2-ROI和花费的相关性/tmp_lw_target_roi_by_day.xlsx')
target_roi_data = target_roi_data.loc[
    (target_roi_data['mediasource'] == 'ALL') & 
    (target_roi_data['country'] == 'ALL') & 
    (target_roi_data['app_package'] == 'com.fun.lastwar.gp'), 
    ['end_date', 'target_roi']
]
target_roi_data['end_date'] = pd.to_datetime(target_roi_data['end_date'])

# 进行成本增幅预测
for index, row in df.iterrows():
    # 获取当前日期和相应数据
    current_date = row['date']
    current_ad_spend = row['ad_spend']
    
    # 预测日期是当前日期的下一天
    predict_date = current_date + pd.Timedelta(days=1)
    
    # 根据预测日期的星期几获取对应的P和Q值
    weekday = predict_date.weekday()  # 使用predict_date计算星期几
    P, Q = get_pq_by_weekday(weekday)  # 基于预测日期计算P和Q值
    
    # 生成花费增幅范围
    cost_increase_range = generate_cost_increase_range(current_ad_spend, P, Q, 10000)
    
    for cost_increase in cost_increase_range:
        # 计算今日预估花费
        today_spend = current_ad_spend * (1 + cost_increase)
        
        # 获取该日期的周末特征
        is_weekend_value = 1 if predict_date.weekday() in [5, 6] else 0  # 计算预测日期是否为周末

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

        # 计算ROI
        roi = predicted_revenue / today_spend if today_spend != 0 else np.nan

        # 获取目标ROI（前一天的目标ROI）
        target_roi_value = target_roi_data.loc[target_roi_data['end_date'] == current_date, 'target_roi']
        target_roi_value = target_roi_value.values[0] if not target_roi_value.empty else np.nan

        if pd.notna(target_roi_value):  # 仅保存目标 ROI 有值的数据
            results.append({
                'date': predict_date,
                'weekday': weekday,
                'lastday_real_spend': current_ad_spend,
                'arppu_daily_mean': row['arppu_daily_mean'],
                'lastday_pud1': row['pud1'],
                'predictedpud1_pct': forecast['yhat'].values[0],
                'predicted_spend': today_spend,
                'predicted_pud1': predicted_pud1,
                'predicted_revenue': predicted_revenue,
                'predicted_roi': roi,
                'target_roi': target_roi_value,
                'predicted_roi_to_target_ratio': roi / target_roi_value if target_roi_value != 0 else np.nan,
                'cost_increase': cost_increase
            })

# 转换为DataFrame
results_df = pd.DataFrame(results)

# 输出查询表单
print("Results DataFrame:")
print(results_df)

# 输出到 Excel 文件
results_df.to_excel('prediction_results.xlsx', index=False)

print(f"Results DataFrame has been saved")

# 可视化结果，生成每个预测日期的图
for date in results_df['date'].unique():
    date_data = results_df[results_df['date'] == date]
    plt.figure(figsize=(12, 6))
    plt.plot(date_data['cost_increase'], date_data['predicted_roi'], label='Predicted ROI')
    plt.axhline(y=date_data['target_roi'].values[0], color='r', linestyle='--', label='Target ROI')
    plt.title(f'Predicted ROI vs Target ROI on {date}')  # 修复日期处理
    plt.xlabel('Cost Increase')
    plt.ylabel('ROI')
    plt.legend()
    plt.show()

# 输出最新数据
# 输出查询表单，只保留最新预测日期
latest_results_df = results_df[results_df['date'] == results_df['date'].max()]

# 计算 predicted_roi_to_target_ratio 与 1 的绝对差值
latest_results_df['abs_diff'] = (latest_results_df['predicted_roi_to_target_ratio'] - 1).abs()

# 找出 abs_diff 最小的行，即预测ROI最接近target ROI
closest_to_1 = latest_results_df.loc[latest_results_df['abs_diff'].idxmin()]

# 输出逻辑
if closest_to_1['abs_diff'] < 0.05:  # 如果有预测ROI接近target ROI的数据
    print(f"推荐安卓大盘花费: {round(closest_to_1['predicted_spend'], 2)}, 首日ROI预计为: {round(closest_to_1['predicted_roi'] * 100, 2)}%, 付费人数为: {int(closest_to_1['predicted_pud1'])}, ARPPU为: {round(closest_to_1['arppu_daily_mean'], 2)}")
else:
    # 按增幅找到最接近target ROI的行
    min_increase_row = latest_results_df.loc[latest_results_df['abs_diff'].idxmin()]
    print(f"今日最大增幅数据为: 安卓大盘花费: {round(min_increase_row['predicted_spend'], 2)}, 首日ROI预计为: {round(min_increase_row['predicted_roi'] * 100, 2)}%, 付费人数为: {int(min_increase_row['predicted_pud1'])}, ARPPU为: {round(min_increase_row['arppu_daily_mean'], 2)}")
    
    # 根据 cost_increase 正负调整输出 half 
    if min_increase_row['cost_increase'] < 0:  
        half_increase = latest_results_df.loc[(latest_results_df['cost_increase'] == P/2 )]
    else:
        half_increase = latest_results_df.loc[(latest_results_df['cost_increase'] == Q/2)]
    if not half_increase.empty:
        half_increase_row = half_increase.iloc[0]
        print(f"今日half增幅数据为: 安卓大盘花费: {round(half_increase_row['predicted_spend'], 2)}, 首日ROI预计为: {round(half_increase_row['predicted_roi'] * 100, 2)}%, 付费人数为: {int(half_increase_row['predicted_pud1'])}, ARPPU为: {round(half_increase_row['arppu_daily_mean'], 2)}")
    
    # 输出 cost_increase = 0 的行
    zero_increase_row = latest_results_df.loc[latest_results_df['cost_increase'] == 0]
    if not zero_increase_row.empty:
        zero_increase_row = zero_increase_row.iloc[0]
        print(f"保持花费不变的数据为: 安卓大盘花费: {round(zero_increase_row['predicted_spend'], 2)}, 首日ROI预计为: {round(zero_increase_row['predicted_roi'] * 100, 2)}%, 付费人数为: {int(zero_increase_row['predicted_pud1'])}, ARPPU为: {round(zero_increase_row['arppu_daily_mean'], 2)}")
        
# 输出历史数据
# 获取所有预测日期的唯一值
unique_dates = results_df['date'].unique()

# 遍历每个预测日期
for date in unique_dates:
    # 过滤出当前日期的数据
    current_day_results = results_df[results_df['date'] == date]

    # 计算 predicted_roi_to_target_ratio 与 1 的绝对差值
    current_day_results['abs_diff'] = (current_day_results['predicted_roi_to_target_ratio'] - 1).abs()

    # 找出 abs_diff 最小的行，即预测ROI最接近target ROI
    closest_to_1 = current_day_results.loc[current_day_results['abs_diff'].idxmin()]

    # 输出逻辑
    print(f"\n预测日期: {date}")
    if closest_to_1['abs_diff'] < 0.05:  # 如果有预测ROI接近target ROI的数据
        print(f"推荐安卓大盘花费: {round(closest_to_1['predicted_spend'], 2)}, 首日ROI预计为: {round(closest_to_1['predicted_roi'] * 100, 2)}%, 付费人数为: {int(closest_to_1['predicted_pud1'])}, ARPPU为: {round(closest_to_1['arppu_daily_mean'], 2)}")
    else:
        # 按增幅找到最接近target ROI的行
        min_increase_row = current_day_results.loc[current_day_results['abs_diff'].idxmin()]
        print(f"今日最大增幅数据为: 安卓大盘花费: {round(min_increase_row['predicted_spend'], 2)}, 首日ROI预计为: {round(min_increase_row['predicted_roi'] * 100, 2)}%, 付费人数为: {int(min_increase_row['predicted_pud1'])}, ARPPU为: {round(min_increase_row['arppu_daily_mean'], 2)}")

        # 计算 half 增幅，即 min_increase_row 的 cost_increase 除以 2
        half_increase_value = min_increase_row['cost_increase'] / 2

        # 找到 half_increase_value 对应的行，使用 np.isclose 来判断浮点值相等
        half_increase = current_day_results.loc[np.isclose(current_day_results['cost_increase'], half_increase_value, atol=1e-5)]

        if not half_increase.empty:
            half_increase_row = half_increase.iloc[0]
            print(f"今日half增幅数据为: 安卓大盘花费: {round(half_increase_row['predicted_spend'], 2)}, 首日ROI预计为: {round(half_increase_row['predicted_roi'] * 100, 2)}%, 付费人数为: {int(half_increase_row['predicted_pud1'])}, ARPPU为: {round(half_increase_row['arppu_daily_mean'], 2)}")
        
        # 输出 cost_increase = 0 的行
        zero_increase_row = current_day_results.loc[np.isclose(current_day_results['cost_increase'], 0, atol=1e-5)]
        if not zero_increase_row.empty:
            zero_increase_row = zero_increase_row.iloc[0]
            print(f"保持花费不变的数据为: 安卓大盘花费: {round(zero_increase_row['predicted_spend'], 2)}, 首日ROI预计为: {round(zero_increase_row['predicted_roi'] * 100, 2)}%, 付费人数为: {int(zero_increase_row['predicted_pud1'])}, ARPPU为: {round(zero_increase_row['arppu_daily_mean'], 2)}")
