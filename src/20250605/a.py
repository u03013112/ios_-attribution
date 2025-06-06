import pandas as pd
import os

def getData():
    filename = '/src/data/a_processed.csv'
    if os.path.exists(filename):
        print('File exists, reading data from', filename)
        df = pd.read_csv(filename)
    else:
        df = pd.read_csv('a.csv')
        df = df[[
            'material_md5',
            'os',
            'mediasource',
            'inventory',
            'cost_value_usd',
            'click',
            'impressions',
            'install',
            'ctr',
            'cvr',
            'cpm',
            'result',
        ]]

        df['ctr'] = df['click'] / df['impressions']
        
        # df['cvr'] = df['install'] / df['click']
        # 如果click为0，则cvr为0，否则计算cvr
        df['cvr'] = df.apply(lambda row: row['install'] / row['click'] if row['click'] > 0 else 0, axis=1)

        df['cpm'] = df['cost_value_usd'] / df['impressions'] * 1000

        # print(df.head())

        # 拆分result，result 是类似"Overall: 62.694 Color: 61.682 Noise: 56.338 Artifact: 58.803 Blur: 60.137 Temporal: 62.39" 的字符串
        # 将其拆分为多个列 Overall, Color, Noise, Artifact, Blur, Temporal
        def split_result(result):
            # Remove spaces around the colon
            result_cleaned = result.replace(' :', ':').replace(': ', ':')
            # Split the cleaned result string by spaces
            parts = result_cleaned.split(' ')
            
            result_dict = {}
            for part in parts:
                if ':' in part:
                    key, value = part.split(':')
                    result_dict[key] = float(value)
            return pd.Series(result_dict)

        df_result = df['result'].astype(str).apply(split_result)
        # print(df_result.head())

        # 将拆分后的结果与原始 DataFrame 合并
        df = pd.concat([df, df_result], axis=1)
        # 删除原始的 result 列
        df.drop(columns=['result'], inplace=True)
        # 保存处理后的 DataFrame
        df.to_csv(filename, index=False)

    return df

def corr():
    df = getData()

    totalDf = df[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm','Overall','Color','Noise','Artifact','Blur','Temporal']]

    
    # 计算'Overall','Color','Noise','Artifact','Blur','Temporal' vs 'cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm' 的相关性
    correlation = totalDf.corr(method='pearson')[['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].loc[
        ['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm']
    ]

    print("Correlation matrix:")
    print(correlation)

    # # 画图，x坐标是['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']升序，每个画一张图
    # # y坐标是'cost_value_usd', 'ctr', 'cvr', 'cpm'，这个需要竖着画4张图，x对齐
    # # 我希望直观的观察到不同指标之间的相关性
    # import matplotlib.pyplot as plt
    # import seaborn as sns

    # for x in ['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']:
    #     totalDf = totalDf.sort_values(by=x, ascending=True)
    #     sns.set(style="whitegrid")
    #     fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(40, 20), sharex=True)
    #     metrics = ['cost_value_usd', 'ctr', 'cvr', 'cpm']
    #     for i, metric in enumerate(metrics):
    #         sns.barplot(x=totalDf[x], y=totalDf[metric], ax=axes[i])
    #         axes[i].set_title(f'{metric} vs {x}')
    #         axes[i].set_xlabel('Overall')
    #         axes[i].set_ylabel(metric)
    #     plt.tight_layout()
    #     plt.savefig(f'/src/data/total_{x}_{metric}_correlation.png')
    #     print(f"Saved plot for {x} vs {metrics} to /src/data/total_{x}_{metric}_correlation.png")
    #     plt.close()

    groupDf = df.groupby(['os', 'mediasource', 'inventory'])
    for name, group in groupDf:
        print(f"\nGroup: {name}")
        group = group[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm', 'Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']]
        group_correlation = group.corr(method='pearson')[['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].loc[
            ['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm']
        ]
        print("Correlation matrix for group:")
        print(group_correlation)

def hist():
    df = getData()
    # 将df中os 的 ‘安卓’ 改名为 'Android'
    df['os'] = df['os'].replace({'安卓': 'Android'})

    totalDf = df[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm','Overall','Color','Noise','Artifact','Blur','Temporal']]

    # 画图，['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']的直方图，每个画一张图
    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set(style="whitegrid")
    for col in ['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']:
        plt.figure(figsize=(10, 6))
        sns.histplot(totalDf[col], kde=True, bins=30)
        plt.title(f'Distribution of {col}')
        plt.xlabel(col)
        plt.ylabel('Frequency')
        plt.savefig(f'/src/data/hist_{col}_distribution.png')
        print(f"Saved distribution plot for {col} to /src/data/hist_{col}_distribution.png")
        plt.close()

    groupDf = df.groupby(['os', 'mediasource', 'inventory'])
    for name, group in groupDf:
        print(f"\nGroup: {name}")
        group = group[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm', 'Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']]
        for col in ['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']:
            plt.figure(figsize=(10, 6))
            sns.histplot(group[col], kde=True, bins=30)
            plt.title(f'Distribution of {col} in group {name}')
            plt.xlabel(col)
            plt.ylabel('Frequency')
            plt.savefig(f'/src/data/hist_{col}_distribution_{name}.png')
            print(f"Saved distribution plot for {col} in group {name} to /src/data/hist_{name}_{col}_distribution.png")
            plt.close()

def main1(N=2):
    df = getData()
    # 总体分析
    totalDf = df[['ctr', 'cvr', 'cpm', 'Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].copy()
    for col in ['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']:
        # 按照分位数分组
        totalDf[f'{col}_group'] = pd.qcut(totalDf[col], N, labels=False)
        
        # 计算每个分组中 'ctr', 'cvr', 'cpm' 的均值以及指标自身的均值
        group_means = totalDf.groupby(f'{col}_group')[[col, 'ctr', 'cvr', 'cpm']].mean()
        
        # 输出结果
        print(f"\n{col} 分组的均值:")
        for group, means in group_means.iterrows():
            print(f"{col} 第{group+1}组: {col} mean: {means[col]:.2f}, ctr mean: {means['ctr']:.4f}, cvr mean: {means['cvr']:.4f}, cpm mean: {means['cpm']:.2f}")
    # 分组分析
    groupDf = df.groupby(['os', 'mediasource', 'inventory'])
    for name, group in groupDf:
        print(f"\nGroup: {name}")
        group = group[['ctr', 'cvr', 'cpm', 'Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].copy()
        
        for col in ['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']:
            # 按照分位数分组
            group[f'{col}_group'] = pd.qcut(group[col], N, labels=False)
            
            # 计算每个分组中 'ctr', 'cvr', 'cpm' 的均值以及指标自身的均值
            group_means = group.groupby(f'{col}_group')[[col, 'ctr', 'cvr', 'cpm']].mean()
            
            # 输出结果
            print(f"\n{col} 分组的均值在组 {name}:")
            for group_num, means in group_means.iterrows():
                print(f"{col} 第{group_num+1}组: {col} mean: {means[col]:.2f}, ctr mean: {means['ctr']:.2f}, cvr mean: {means['cvr']:.2f}, cpm mean: {means['cpm']:.2f}")



if __name__ == "__main__":
    # corr()
    # hist()
    main1(N=4)
