import pandas as pd
import numpy as np
import os

# 定义前缀分组
prefix_groups = [
    {
        'name': '付费用户不分组',
        'prefixList': [
            'lw20241030_pudt_1',
            'lw20241030_pudt_1_media',
            'lw20241030_pudt_1_country',
            'lw20241030_pudt_1_media_country'
        ]
    },
    {
        'name': '付费用户分2组',
        'prefixList': [
            'lw20241030_pudt_2',
            'lw20241030_pudt_2_media',
            'lw20241030_pudt_2_country',
            'lw20241030_pudt_2_media_country'
        ]
    },
    {
        'name': '付费用户分3组',
        'prefixList': [
            'lw20241030_pudt_3',
            'lw20241030_pudt_3_media',
            'lw20241030_pudt_3_country',
            'lw20241030_pudt_3_media_country'
        ]
    }
]

# 读取并整理结论
def read_and_process_csv(prefix):
    filename = f'/src/data/{prefix}_mape_by_pay_user_group_by_day.csv'
    if not os.path.exists(filename):
        print(f"文件 {filename} 不存在，跳过。")
        return None

    data = pd.read_csv(filename)
    data['ds'] = pd.to_datetime(data['ds'])
    data['week'] = data['ds'].dt.isocalendar().week

    return data

def calculate_mape(actual, predicted):
    return np.abs((actual - predicted) / actual) * 100

def process_group(prefix_group):
    results = []

    for prefix in prefix_group['prefixList']:
        data = read_and_process_csv(prefix)
        if data is None:
            continue

        # 总误差（天）
        total_mape_day = data.groupby(['country', 'media', 'ds']).apply(
            lambda x: calculate_mape(x['actual_revenue'].sum(), x['predicted_revenue'].sum()).mean()
        ).groupby(['country', 'media']).mean().reset_index(name='total_mape_day')

        # 总误差（周）
        total_mape_week = data.groupby(['country', 'media', 'week']).apply(
            lambda x: calculate_mape(x['actual_revenue'].sum(), x['predicted_revenue'].sum()).mean()
        ).groupby(['country', 'media']).mean().reset_index(name='total_mape_week')

        # 分组误差（天）
        group_mape_day = data.groupby(['country', 'media', 'pay_user_group_name', 'ds']).apply(
            lambda x: calculate_mape(x['actual_revenue'].sum(), x['predicted_revenue'].sum()).mean()
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_mape_day')

        # 分组误差（周）
        group_mape_week = data.groupby(['country', 'media', 'pay_user_group_name', 'week']).apply(
            lambda x: calculate_mape(x['actual_revenue'].sum(), x['predicted_revenue'].sum()).mean()
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_mape_week')

        # 分组权重（天）
        group_weight_day = data.groupby(['country', 'media', 'pay_user_group_name', 'ds']).apply(
            lambda x: x['actual_revenue'].sum() / data.groupby(['country', 'media', 'ds'])['actual_revenue'].sum().loc[(x['country'].iloc[0], x['media'].iloc[0], x['ds'].iloc[0])]
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_weight_day')

        # 分组权重（周）
        group_weight_week = data.groupby(['country', 'media', 'pay_user_group_name', 'week']).apply(
            lambda x: x['actual_revenue'].sum() / data.groupby(['country', 'media', 'week'])['actual_revenue'].sum().loc[(x['country'].iloc[0], x['media'].iloc[0], x['week'].iloc[0])]
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_weight_week')

        # 分组pu误差（天）
        group_pu_mape_day = data.groupby(['country', 'media', 'pay_user_group_name', 'ds']).apply(
            lambda x: calculate_mape(x['actual_pu'].sum(), x['predicted_pu'].sum()).mean()
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_pu_mape_day')

        # 分组pu误差（周）
        group_pu_mape_week = data.groupby(['country', 'media', 'pay_user_group_name', 'week']).apply(
            lambda x: calculate_mape(x['actual_pu'].sum(), x['predicted_pu'].sum()).mean()
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_pu_mape_week')

        # 分组arppu误差（天）
        group_arppu_mape_day = data.groupby(['country', 'media', 'pay_user_group_name', 'ds']).apply(
            lambda x: calculate_mape(x['actual_arppu'].mean(), x['predicted_arppu'].mean()).mean()
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_arppu_mape_day')

        # 分组arppu误差（周）
        group_arppu_mape_week = data.groupby(['country', 'media', 'pay_user_group_name', 'week']).apply(
            lambda x: calculate_mape(x['actual_arppu'].mean(), x['predicted_arppu'].mean()).mean()
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_arppu_mape_week')

        # 合并结果
        merged = total_mape_day.merge(total_mape_week, on=['country', 'media'], how='left')
        merged = merged.merge(group_mape_day, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_mape_week, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_weight_day, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_weight_week, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_pu_mape_day, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_pu_mape_week, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_arppu_mape_day, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_arppu_mape_week, on=['country', 'media', 'pay_user_group_name'], how='left')

        merged['prefix'] = prefix
        merged['分组名'] = prefix_group['name']

        results.append(merged)

    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame()

# 主函数
def main():
    all_results = []

    for group in prefix_groups:
        result = process_group(group)
        if not result.empty:
            all_results.append(result)

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df.to_csv('/src/data/final_results.csv', index=False)
        print("结果已保存到 /src/data/final_results.csv")
    else:
        print("没有生成任何结果。")

if __name__ == '__main__':
    main()
