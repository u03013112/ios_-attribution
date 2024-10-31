import pandas as pd
import os

def read_mape_files(prefix_list, base_path='/src/data'):
    """
    读取所有生成的MAPE文件，并将结果汇总到一个表格中。
    """
    results = []

    for prefix in prefix_list:
        # 读取按安装日期、country 和 media 的平均 MAPE
        daily_mape_file = os.path.join(base_path, f'{prefix}_mape_by_install_day_country_media_avg.csv')
        weekly_mape_file = os.path.join(base_path, f'{prefix}_mape_by_install_week_country_media_avg.csv')

        if os.path.exists(daily_mape_file) and os.path.exists(weekly_mape_file):
            daily_mape_df = pd.read_csv(daily_mape_file)
            weekly_mape_df = pd.read_csv(weekly_mape_file)

            # 合并日和周的MAPE数据
            merged_df = pd.merge(
                daily_mape_df,
                weekly_mape_df,
                on=['country', 'media'],
                suffixes=('_daily', '_weekly')
            )

            # 添加前缀列
            merged_df['prefix'] = prefix

            # 选择并重命名所需列
            merged_df = merged_df[[
                'prefix', 'country', 'media',
                'avg_daily_mape_pu', 'avg_daily_mape_arppu', 'avg_daily_mape_revenue',
                'avg_weekly_mape_pu', 'avg_weekly_mape_arppu', 'avg_weekly_mape_revenue'
            ]]

            # 将所有 MAPE 值除以 100
            mape_columns = [
                'avg_daily_mape_pu', 'avg_daily_mape_arppu', 'avg_daily_mape_revenue',
                'avg_weekly_mape_pu', 'avg_weekly_mape_arppu', 'avg_weekly_mape_revenue'
            ]
            merged_df[mape_columns] = merged_df[mape_columns] / 100

            results.append(merged_df)
        else:
            print(f"文件缺失: {daily_mape_file} 或 {weekly_mape_file}")

    # 合并所有结果
    if results:
        final_df = pd.concat(results, ignore_index=True)
    else:
        print("没有任何结果生成。")
        return None

    return final_df

def main():
    # 配置前缀列表
    prefix_list = [
        'lw20241030_pudt_all1',
        'lw20241030_pudt_all1_media',
        'lw20241030_pudt_all1_country',
        'lw20241030_pudt_all1_media_country',
        'lw20241030_pudt_two2',
        'lw20241030_pudt_two2_media',
        'lw20241030_pudt_two2_country',
        'lw20241030_pudt_two2_media_country'
    ]

    # 读取并汇总MAPE结果
    final_df = read_mape_files(prefix_list)

    if final_df is not None:
        # 保存最终结果到CSV
        final_filename = '/src/data/final_mape_comparison.csv'
        final_df.to_csv(final_filename, index=False)
        print(f"最终结果已保存到 {final_filename}")

        # 打印最终结果
        print(final_df)

if __name__ == '__main__':
    main()
