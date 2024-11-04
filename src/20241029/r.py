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
    },
    {
        'name': '付费用户分50组',
        'prefixList': [
            'lw20241101_pudt_50',
            'lw20241101_pudt_50_media',
            'lw20241101_pudt_50_country',
            'lw20241101_pudt_50_media_country'
        ]
    },
    {
        'name': '付费用户分33_66组',
        'prefixList': [
            'lw20241101_pudt_33_66',
            'lw20241101_pudt_33_66_media',
            'lw20241101_pudt_33_66_country',
            'lw20241101_pudt_33_66_media_country'
        ]
    },
    {
        'name': '付费用户分25_50_75组',
        'prefixList': [
            'lw20241101_pudt_25_50_75',
            'lw20241101_pudt_25_50_75_media',
            'lw20241101_pudt_25_50_75_country',
            'lw20241101_pudt_25_50_75_media_country'
        ]
    }
]

# 读取并整理结论
def read_and_process_csv(prefix):
    filename = f'/src/data/{prefix}_mape_by_pay_user_group_by_day.csv'
    if not os.path.exists(filename):
        print(f"文件 {filename} 不存在，跳过。")
        return None

    print(f"处理文件: {filename}")
    data = pd.read_csv(filename)
    data['ds'] = pd.to_datetime(data['ds'])
    data['week'] = data['ds'].dt.isocalendar().week

    return data

def calculate_mape(actual, predicted):
    with np.errstate(divide='ignore', invalid='ignore'):
        mape = np.abs((actual - predicted) / actual)
        mape = np.where(actual == 0, 0, mape)
    return mape

def process_group(prefix_group):
    results = []

    for prefix in prefix_group['prefixList']:
        data = read_and_process_csv(prefix)
        if data is None:
            continue

        # 获取所有的 pay_user_group_name
        pay_user_groups = data['pay_user_group_name'].unique()

        # 总误差（天）
        total_mape_day = data.groupby(['country', 'media', 'ds']).apply(
            lambda x: calculate_mape(x['actual_revenue'].sum(), x['predicted_revenue'].sum()).mean()
        ).groupby(['country', 'media']).mean().reset_index(name='total_mape_day')

        # 总误差（周）
        total_mape_week = data.groupby(['country', 'media', 'week']).apply(
            lambda x: calculate_mape(x['actual_revenue'].sum(), x['predicted_revenue'].sum()).mean()
        ).groupby(['country', 'media']).mean().reset_index(name='total_mape_week')

        # 创建一个包含所有 pay_user_group_name 的 DataFrame
        pay_user_group_df = pd.DataFrame({'pay_user_group_name': pay_user_groups})

        # 为每个 country 和 media 分配 pay_user_group_name
        total_mape_day = total_mape_day.assign(key=1).merge(pay_user_group_df.assign(key=1), on='key').drop('key', axis=1)
        total_mape_week = total_mape_week.assign(key=1).merge(pay_user_group_df.assign(key=1), on='key').drop('key', axis=1)

        # 分组误差（天）
        group_mape_day = data.groupby(['country', 'media', 'pay_user_group_name', 'ds']).apply(
            lambda x: calculate_mape(x['actual_revenue'].sum(), x['predicted_revenue'].sum()).mean()
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_mape_day')

        # 分组误差（周）
        group_mape_week = data.groupby(['country', 'media', 'pay_user_group_name', 'week']).apply(
            lambda x: calculate_mape(x['actual_revenue'].sum(), x['predicted_revenue'].sum()).mean()
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='group_mape_week')

        # 分组收入金额占比（天）
        group_weight_day = data.groupby(['country', 'media', 'pay_user_group_name', 'ds']).apply(
            lambda x: x['actual_revenue'].sum() / data.groupby(['country', 'media', 'ds'])['actual_revenue'].sum().loc[(x['country'].iloc[0], x['media'].iloc[0], x['ds'].iloc[0])]
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='收入金额占比')

        # 分组付费用户数占比（天）
        group_pu_weight_day = data.groupby(['country', 'media', 'pay_user_group_name', 'ds']).apply(
            lambda x: x['actual_pu'].sum() / data.groupby(['country', 'media', 'ds'])['actual_pu'].sum().loc[(x['country'].iloc[0], x['media'].iloc[0], x['ds'].iloc[0])]
        ).groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='付费用户数占比')

        # 分组付费用户数（天）
        group_pu_count_day = data.groupby(['country', 'media', 'pay_user_group_name', 'ds'])['actual_pu'].sum().groupby(['country', 'media', 'pay_user_group_name']).mean().reset_index(name='付费用户数')

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
        merged = total_mape_day.merge(total_mape_week, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_mape_day, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_mape_week, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_weight_day, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_pu_weight_day, on=['country', 'media', 'pay_user_group_name'], how='left')
        merged = merged.merge(group_pu_count_day, on=['country', 'media', 'pay_user_group_name'], how='left')
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

        # 调整列顺序
        final_df = final_df[['country', 'media', '分组名', 'prefix', 'total_mape_day', 'total_mape_week', 'pay_user_group_name', 'group_mape_day', 'group_mape_week', '收入金额占比', '付费用户数占比', '付费用户数', 'group_pu_mape_day', 'group_pu_mape_week', 'group_arppu_mape_day', 'group_arppu_mape_week']]
        final_df = final_df.sort_values(by=['分组名', 'prefix', 'country', 'media', 'pay_user_group_name'])

        final_df.to_csv('/src/data/final_results.csv', index=False)
        print("结果已保存到 /src/data/final_results.csv")
    else:
        print("没有生成任何结果。")

if __name__ == '__main__':
    main()
