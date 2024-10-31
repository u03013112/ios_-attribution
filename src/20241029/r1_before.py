import pandas as pd
import numpy as np
import os

def calculate_arppu_and_mape(df):
    """
    重新计算 actual_arppu 和 predicted_arppu，并计算 MAPE。
    """
    df['actual_arppu'] = df['actual_revenue'] / df['actual_pu']
    df['predicted_arppu'] = df['predicted_revenue'] / df['predicted_pu']

    df['mape_arppu'] = (np.abs((df['actual_arppu'] - df['predicted_arppu']) / df['actual_arppu']) * 100).replace([np.inf, -np.inf], np.nan)
    return df

def process_file(file_path, output_path):
    """
    处理单个文件，重新计算 ARPPU 和 MAPE，并保存结果。
    """
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df = calculate_arppu_and_mape(df)
        df.to_csv(output_path, index=False)
        print(f"结果已保存到 {output_path}")
    else:
        print(f"文件不存在: {file_path}")

def calculate_avg_mape(input_path, output_path, group_by_columns, mape_columns):
    """
    计算并保存按指定列的平均 MAPE。
    """
    if os.path.exists(input_path):
        df = pd.read_csv(input_path)
        avg_mape = df.groupby(group_by_columns).agg(
            avg_mape_pu=pd.NamedAgg(column='mape_pu', aggfunc='mean'),
            avg_mape_revenue=pd.NamedAgg(column='mape_revenue', aggfunc='mean'),
            avg_mape_arppu=pd.NamedAgg(column='mape_arppu', aggfunc='mean')
        ).reset_index()
        avg_mape.columns = group_by_columns + mape_columns
        avg_mape.to_csv(output_path, index=False)
        print(f"平均 MAPE 结果已保存到 {output_path}")
    else:
        print(f"文件不存在: {input_path}")

def main():
    base_path = '/src/data'
    prefixes = [
        # 'lw20241030_pudt_two2',
        # 'lw20241030_pudt_two2_media',
        # 'lw20241030_pudt_two2_country',
        # 'lw20241030_pudt_two2_media_country',
        'lw20241030_pudt_3',
        'lw20241030_pudt_3_media',
        'lw20241030_pudt_3_country',
        'lw20241030_pudt_3_media_country'
    ]

    for prefix in prefixes:
        print(f"处理前缀: {prefix}")

        # 处理按安装日期（天）的文件
        day_file = os.path.join(base_path, f'{prefix}_mape_by_install_day.csv')
        day_output_file = os.path.join(base_path, f'{prefix}_mape_by_install_day_recalculated.csv')
        process_file(day_file, day_output_file)

        # 计算并保存按 pay_user_group_name、country 和 media 的天平均 MAPE
        day_avg_output_file = os.path.join(base_path, f'{prefix}_mape_by_install_day_country_media_avg.csv')
        calculate_avg_mape(day_output_file, day_avg_output_file, ['country', 'media'], ['avg_daily_mape_pu', 'avg_daily_mape_revenue', 'avg_daily_mape_arppu'])

        # 处理按安装日期（周）的文件
        week_file = os.path.join(base_path, f'{prefix}_mape_by_install_week.csv')
        week_output_file = os.path.join(base_path, f'{prefix}_mape_by_install_week_recalculated.csv')
        process_file(week_file, week_output_file)

        # 计算并保存按 pay_user_group_name、country 和 media 的周平均 MAPE
        week_avg_output_file = os.path.join(base_path, f'{prefix}_mape_by_install_week_country_media_avg.csv')
        calculate_avg_mape(week_output_file, week_avg_output_file, ['country', 'media'], ['avg_weekly_mape_pu', 'avg_weekly_mape_revenue', 'avg_weekly_mape_arppu'])

if __name__ == '__main__':
    main()
