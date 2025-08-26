# 结论整理，写代码简单一点
import pandas as pd

filenameList = [
    'best_methods_rawDf.csv',
    'hierarchical_best_methods_groupDf2.csv',
    'hierarchical_best_methods_groupDf4.csv',
    'hierarchical_best_methods_groupDf8.csv',
    'hierarchical_best_methods_groupDf16.csv',
    'hierarchical_best_methods_groupDf32.csv',
]

# 将filenameList中的文件 加上路径 '/src/data/' 前缀
# 并读取为DataFrame,只保留app_package，country_group，mediasource，weekly_mape列
# 用app_package，country_group，mediasource，来做merge，weekly_mape 添加不同的后缀，文件名中下划线最后一段
# 最终保存csv /src/data/d.csv
def read_and_process_file(filename):
    df = pd.read_csv(f'/src/data/{filename}')
    df = df[['app_package', 'country_group', 'mediasource', 'weekly_mape']]
    suffix = filename.split('_')[-1].replace('.csv', '')
    df.rename(columns={'weekly_mape': f'weekly_mape_{suffix}'}, inplace=True)
    return df

def main():
    dfs = [read_and_process_file(filename) for filename in filenameList]
    
    # Merge all DataFrames on app_package, country_group, mediasource
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=['app_package', 'country_group', 'mediasource'], how='outer')
    
    # Save the merged DataFrame to a CSV file
    merged_df.to_csv('/src/data/d.csv', index=False)

if __name__ == "__main__":
    main()
    print("Data processing complete. Output saved to /src/data/d.csv")