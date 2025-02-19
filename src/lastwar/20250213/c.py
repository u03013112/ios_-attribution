# 使用ds，尝试推测趋势

import numpy as np
import pandas as pd

def getData():
    df = pd.read_csv('lastwar_分服流水每天_20240101_20250217.csv')

    # 将"时间"列中的字符串转换为时间类型，其中有一些类似"阶段汇总"的字符串，直接整行删除
    df['时间'] = pd.to_datetime(df['时间'], errors='coerce')
    df = df.dropna(subset=['时间'])

    # 列改名： "时间" -> "day", "服务器ID" -> "server_id", "S新支付.美元付费金额 - USD(每日汇率)总和" -> "revenue","S新支付.触发用户数" -> "pay_users","S登录.触发用户数" -> "login_users"
    df = df.rename(columns={
        '时间': 'day', 
        '服务器ID': 'server_id', 
        'S新支付.美元付费金额 - USD(每日汇率)总和': 'revenue', 
    })

    # 将 服务器ID 为 空 的行删除
    df = df.dropna(subset=['server_id'])
    df = df[df['server_id'] != '(null)']

    # 将服务器ID转换为整数，无法转换的直接扔掉
    def convert_server_id(server_id):
        try:
            return int(server_id[3:])
        except:
            return np.nan

    df['server_id_int'] = df['server_id'].apply(convert_server_id)
    df = df.dropna(subset=['server_id_int'])
    df['server_id_int'] = df['server_id_int'].astype(int)

    # 服务器ID 只统计到 'APS1188' 服务器
    df = df[df['server_id_int'] <= 1188]

    # 将无法转换为浮点数的字符串替换为 NaN，然后再用 0 替换 NaN
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
    
    return df

# def main():
#     df = getData()
    
#     df = df[(df['server_id_int'] >= 3) & (df['server_id_int'] <= 36)]

#     # 按服务器分组处理
#     results = []
#     for server_id, group in df.groupby('server_id_int'):
#         server_df = group.sort_values('day').reset_index(drop=True)
        
#         # 跳过数据量不足的服务器
#         if len(server_df) < 30:
#             continue
        
#         # 提取时间序列
#         series = server_df['revenue'].values
        
#         # 计算趋势斜率（USD/天）
#         slope = calculate_trend_slope(series)
        
#         # 趋势显著性检验
#         p_value = kendalltau(np.arange(len(series)), series).pvalue
        
#         # 生命周期阶段分类
#         if slope < -500 and p_value < 0.05:
#             stage = "衰退期"
#         elif slope < -500:
#             stage = "疑似衰退"
#         elif -500 <= slope <= 300:
#             stage = "平稳期"
#         else:
#             stage = "增长期"
        
#         # 记录结果
#         results.append({
#             'server_id': f"APS{server_id}",
#             'slope': slope,
#             'p_value': p_value,
#             'stage': stage,
#             'last_revenue': series[-1],
#             'days': len(series)
#         })
    
#     # 转换为DataFrame
#     result_df = pd.DataFrame(results)
    
#     # 保存结果到CSV
#     result_df.to_csv('/src/data/2025_02_19_server_lifecycle_analysis.csv', index=False)
    
#     # 打印关键统计
#     print(f"分析完成，共处理{len(result_df)}个服务器")
#     print("阶段分布：")
#     print(result_df['stage'].value_counts())
    
#     # # 可视化示例服务器（APS27）
#     # sample_server = df[df['server_id_int'] == 27]
#     # plot_lifecycle(sample_server)

# 添加依赖函数
from scipy.stats import kendalltau
from sklearn.linear_model import TheilSenRegressor
import matplotlib.pyplot as plt

def calculate_trend_slope(series):
    """计算每日收入变化率"""
    X = np.arange(len(series)).reshape(-1, 1)
    return TheilSenRegressor(random_state=42).fit(X, series).coef_[0]

def plot_lifecycle(server_data):
    """可视化单个服务器生命周期"""
    fig, ax = plt.subplots(figsize=(12,6))
    
    # 原始数据
    ax.plot(server_data['day'], server_data['revenue'], 
            label='实际收入', color='#2c7bb6')
    
    # 趋势线
    X = np.arange(len(server_data)).reshape(-1,1)
    model = TheilSenRegressor().fit(X, server_data['revenue'])
    ax.plot(server_data['day'], model.predict(X),
            label=f'趋势线 ({model.coef_[0]:.1f} USD/天)',
            linestyle='--', color='#d7191c')
    
    # 格式设置
    ax.set_title(f"{server_data['server_id'].iloc[0]} 生命周期诊断")
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{server_data['server_id'].iloc[0]}_analysis.png")
    plt.close()

import numpy as np
import pandas as pd
from ruptures import Pelt
from sklearn.linear_model import LinearRegression

def detect_phases(server_data):
    """修复索引越界问题的版本"""
    # 预处理
    ts = server_data.set_index('day')['revenue'].sort_index()
    ts = ts.resample('W-MON').mean().ffill()
    
    # 突变点检测（修复索引问题）
    algo = Pelt(model="rbf", min_size=3).fit(ts.values)
    change_points = algo.predict(pen=5)
    
    # 关键修复：过滤无效索引
    valid_change_points = [cp for cp in change_points if cp < len(ts)]
    if not valid_change_points:
        print("未检测到显著阶段变化")
        return
    
    # 添加初始起点和终点
    all_points = [0] + valid_change_points + [len(ts)]
    
    # 阶段划分
    phases = []
    for i in range(len(all_points)-1):
        start_idx = all_points[i]
        end_idx = all_points[i+1] - 1  # 防止越界
        segment = ts.iloc[start_idx:end_idx+1]
        
        # 跳过空段
        if len(segment) < 2:
            continue
            
        # 趋势分析
        slope = LinearRegression().fit(np.arange(len(segment)).reshape(-1,1), segment.values).coef_[0]
        
        # 阶段分类（优化逻辑）
        if slope > 0 and slope > ts.std()*0.3:
            phase_type = "上升期"
        elif slope < 0 and abs(slope) > ts.std()*0.3:
            phase_type = "下降期"
        else:
            phase_type = "平稳期"
            
        # 检测断崖冲击（优化阈值）
        last_2_weeks = segment[-2:].values
        if len(last_2_weeks) == 2:
            change_rate = (last_2_weeks[1] - last_2_weeks[0]) / (abs(last_2_weeks[0]) + 1e-5)
            if abs(change_rate) > 0.25:  # 25%变化阈值
                cliff_type = "断崖冲击（上升）" if change_rate > 0 else "断崖冲击（下降）"
                phase_type += f" → {cliff_type}"
        
        phases.append({
            "start": segment.index[0].strftime('%Y-%m-%d'),
            "end": segment.index[-1].strftime('%Y-%m-%d'),
            "type": phase_type,
            "slope": slope
        })
    
    # 打印结果（优化输出）
    print("服务器阶段划分报告（修复版）：")
    print(f"数据总周数: {len(ts)}")
    print(f"检测到有效变点: {valid_change_points}")
    print("-"*50)
    for i, phase in enumerate(phases, 1):
        duration = (pd.to_datetime(phase['end']) - pd.to_datetime(phase['start'])).days // 7
        print(f"阶段 {i} [{duration}周]")
        print(f"日期: {phase['start']} 至 {phase['end']}")
        print(f"类型: {phase['type']}")
        print(f"趋势斜率: {phase['slope']:.1f} USD/周")
        print(f"期初收入: {ts.loc[phase['start']]:.0f} USD")
        print(f"期末收入: {ts.loc[phase['end']]:.0f} USD")
        print("-"*50)

# 新断崖检测条件
def is_cliff(segment):
    # 要求同时满足：
    # 1. 最后一周变化率 > 40%
    # 2. 整体趋势与突变方向一致
    # 3. 突变后维持新水平至少2周
    
    last_2 = segment[-2:].values
    if len(last_2) < 2:
        return False
    
    change_rate = (last_2[1] - last_2[0]) / (abs(last_2[0]) + 1e-5)
    slope = LinearRegression().fit(np.arange(len(segment)), segment).coef_[0]
    
    # 方向一致性检测
    direction_consistent = (change_rate > 0 and slope > 0) or (change_rate < 0 and slope < 0)
    
    # 持续性检测
    if len(segment) >= 4:
        post_change = segment[-2:].mean()
        pre_change = segment[-4:-2].mean()
        sustain_check = abs(post_change - pre_change) > ts.mad()*0.5
    else:
        sustain_check = True
    
    return abs(change_rate) > 0.4 and direction_consistent and sustain_check

def validate_with_daily_data(server_data, phase_result):
    """用日数据验证周检测结果"""
    daily = server_data.set_index('day')['revenue']
    
    for phase in phase_result:
        start = pd.to_datetime(phase['start'])
        end = pd.to_datetime(phase['end'])
        
        # 提取日数据
        phase_daily = daily[start:end]
        
        # 检测日级断崖
        daily_changes = phase_daily.pct_change().abs()
        if daily_changes.max() > 0.6:  # 单日跌幅>60%
            print(f"阶段 {phase['start']}-{phase['end']} 存在日级断崖")
            print(f"最大单日变化: {daily_changes.idxmax()} {daily_changes.max():.0%}")

def detect_phases_enhanced(server_data):
    # 预处理（增加日数据备份）
    daily_ts = server_data.set_index('day')['revenue'].sort_index()
    ts = daily_ts.resample('W-MON').mean().ffill()
    
    # 变点检测（优化参数）
    algo = Pelt(model="rbf", min_size=2).fit(ts.values)
    change_points = algo.predict(pen=3)
    valid_points = [cp for cp in change_points if 2 < cp < len(ts)-2] or [len(ts)//2]
    
    # 阶段划分（新逻辑）
    phases = []
    prev_idx = 0
    for cp in valid_points:
        segment = ts.iloc[prev_idx:cp]
        slope = LinearRegression().fit(np.arange(len(segment)), segment).coef_[0]
        
        # 新趋势分类
        if abs(slope) > trend_threshold:
            phase_type = "上升期" if slope > 0 else "下降期"
        elif abs(slope) > stable_threshold:
            phase_type = "波动期"
        else:
            phase_type = "平稳期"
        
        # 新断崖检测
        cliff_detected = False
        if len(segment) >= 4 and is_cliff(segment):
            cliff_type = "上升" if segment[-1] > segment[-2] else "下降"
            phase_type += f" ★断崖{cliff_type}★"
            cliff_detected = True
        
        phases.append({
            "start": segment.index[0].strftime('%Y-%m-%d'),
            "end": segment.index[-1].strftime('%Y-%m-%d'),
            "type": phase_type,
            "slope": slope,
            "is_cliff": cliff_detected
        })
        prev_idx = cp
    
    # 日数据验证
    validate_with_daily_data(daily_ts, phases)
    
    return phases

# 使用示例（假设df是你的原始数据）
def main():
    df = getData()  # 你的数据读取函数
    # 示例调用（确保数据排序）
    server_10 = df[df['server_id_int'] == 10].sort_values('day').reset_index(drop=True)
    # detect_phases(server_10)
    detect_phases_enhanced(server_10)

if __name__ == '__main__':
    main()