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
        df['cvr'] = df['install'] / df['click']
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

def main():
    df = getData()

    totalDf = df[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm','Overall','Color','Noise','Artifact','Blur','Temporal']]

    
    # 计算'Overall','Color','Noise','Artifact','Blur','Temporal' vs 'cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm' 的相关性
    correlation = totalDf.corr(method='pearson')[['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].loc[
        ['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm']
    ]

    print("Correlation matrix:")
    print(correlation)

    groupDf = df.groupby(['os', 'mediasource', 'inventory'])
    for name, group in groupDf:
        print(f"\nGroup: {name}")
        group = group[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm', 'Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']]
        group_correlation = group.corr(method='pearson')[['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].loc[
            ['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm']
        ]
        print("Correlation matrix for group:")
        print(group_correlation)

    

if __name__ == "__main__":
    main()
