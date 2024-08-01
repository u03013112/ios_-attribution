# 搭建视频标签与是否畅销有关

# 分维度 1、平台 2、媒体 3、国家 4、平台 + 媒体 5、平台 + 国家 6、媒体 + 国家 7、平台 + 媒体 + 国家

# 对畅销进行判断，打标签，畅销为1，不畅销为0

# 将视频的标签与畅销标签进行合并，再计算是否相关

import os
import numpy as np
import pandas as pd
from datetime import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getTopVideoData(installTimeStart = '20240601',installTimeEnd = '20240630'):
    filename = f'/src/data/zk2/lwTopVideoData_{installTimeStart}_{installTimeEnd}.csv'

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # 获得用户信息，这里要额外获得归因信息，精确到campaign
        sql = f'''
select
    install_day,
    app_package,
    mediasource,
    country_levels as country,
    sum(cost_value_usd) as cost,
    material_name
from
    rg_bi.dws_material_overseas_data_public
where
    app = '502'
    and material_type = '视频'
    and install_day between {installTimeStart} and {installTimeEnd}
group by
    install_day,
    app_package,
    mediasource,
    country_levels,
    material_name
;
        '''
        df = execSql(sql)
        df.to_csv(filename,index=False)

    return df


# dim 是维度，取值为 appPackage、mediasource、country、appPackage+mediasource、appPackage+country、mediasource+country、appPackage+mediasource+country
def getTopVideoDataWithLabel(installTimeStart='20240601', installTimeEnd='20240630', dim='appPackage', cost_threshold=0.2):
    dataDf = getTopVideoData(installTimeStart, installTimeEnd)

    # 根据dim进行分组
    if dim == 'appPackage':
        grouped = dataDf.groupby(['app_package'])
    elif dim == 'mediasource':
        grouped = dataDf.groupby(['mediasource'])
    elif dim == 'country':
        grouped = dataDf.groupby(['country'])
    elif dim == 'appPackage+mediasource':
        grouped = dataDf.groupby(['app_package', 'mediasource'])
    elif dim == 'appPackage+country':
        grouped = dataDf.groupby(['app_package', 'country'])
    elif dim == 'mediasource+country':
        grouped = dataDf.groupby(['mediasource', 'country'])
    elif dim == 'appPackage+mediasource+country':
        grouped = dataDf.groupby(['app_package', 'mediasource', 'country'])
    else:
        raise ValueError("Invalid dimension")

    result_list = []

    for name, group in grouped:
        total_cost = group['cost'].sum()

        material_grouped = group.groupby('material_name').agg({
            'cost': 'sum'
        }).reset_index()

        material_grouped['cost_ratio'] = material_grouped['cost'] / total_cost
        material_grouped['is_top'] = material_grouped['cost_ratio'] > cost_threshold

        for _, row in material_grouped.iterrows():
            result_dict = {
                'material_name': row['material_name'],
                'is_top': int(row['is_top'])
            }
            if dim == 'appPackage':
                result_dict['app_package'] = name
            elif dim == 'mediasource':
                result_dict['mediasource'] = name
            elif dim == 'country':
                result_dict['country'] = name
            elif dim == 'appPackage+mediasource':
                result_dict['app_package'] = name[0]
                result_dict['mediasource'] = name[1]
            elif dim == 'appPackage+country':
                result_dict['app_package'] = name[0]
                result_dict['country'] = name[1]
            elif dim == 'mediasource+country':
                result_dict['mediasource'] = name[0]
                result_dict['country'] = name[1]
            elif dim == 'appPackage+mediasource+country':
                result_dict['app_package'] = name[0]
                result_dict['mediasource'] = name[1]
                result_dict['country'] = name[2]

            result_list.append(result_dict)

    result_df = pd.DataFrame(result_list)
    return result_df

def getVideoParams01(installTimeStart = '20240601',installTimeEnd = '20240630'):
    filename = f'/src/data/zk2/lwVideoParams01_{installTimeStart}_{installTimeEnd}.csv'

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # 获得用户信息，这里要额外获得归因信息，精确到campaign
        sql = f'''
@data1 :=
select
    material_name,
    original_name,
    material_md5
from
    rg_bi.dws_material_overseas_data_public
where
    app = '502'
    and material_type = '视频'
    and install_day between '{installTimeStart}'
    and '{installTimeEnd}'
group by
    material_name,
    original_name,
    material_md5;

@data2 :=
select
    material_md5,
    tag_id
from
    ods_material_tag_relation_v3;

@data3 :=
select
    tag_id,
    tag_name,
    level_tag_name
from
    ods_material_tag_v3;

@data4 :=
select
    d1.material_name,
    d1.original_name,
    d2.tag_id
from
    @data1 d1
    join @data2 d2 on d1.material_md5 = d2.material_md5;

select
    d4.material_name,
    max(d4.original_name) as original_name,
    max(d3.level_tag_name) as level_tag_name
from
    @data4 d4
    join @data3 d3 on d4.tag_id = d3.tag_id
group by
    d4.material_name;
        '''
        df = execSql(sql)
        df.to_csv(filename,index=False)

    return df

def getCorrelation(topVideoDf, videoParamsDf):
    # 合并数据
    df = pd.merge(topVideoDf, videoParamsDf, on='material_name', how='left')

    # 提取维度列
    dimension_columns = topVideoDf.columns.difference(['material_name', 'is_top'])

    # 将videoParamsDf中的其他列都视为特征
    feature_columns = videoParamsDf.columns.difference(['material_name'])

    # 初始化结果列表
    correlation_results = []

    # 按照维度分组
    grouped = df.groupby(list(dimension_columns))

    for name, group in grouped:
        # 将特征转化为one hot模式
        group_one_hot = pd.get_dummies(group, columns=feature_columns)

        group_one_hot.to_csv(f'/src/data/zk2/lw_group_one_hot_{name}.csv', index=False)

        # 计算并得出与畅销label相关性
        correlation_matrix = group_one_hot.corr()

        # 获取与'is_top'列的相关性
        is_top_correlation = correlation_matrix['is_top'].drop('is_top')

        # 记录结果
        for feature, correlation in is_top_correlation.items():
            feature_info = feature.split('_')
            feature_name = feature_info[0]
            feature_value = '_'.join(feature_info[1:])
            result = {
                'dimension': name,
                'feature': feature_name,
                'value': feature_value,
                'correlation': correlation
            }
            correlation_results.append(result)

    # 转换结果为DataFrame
    correlation_df = pd.DataFrame(correlation_results)

    # 将相关性保存到csv文件中
    correlation_df.to_csv('/src/data/zk2/lw_is_top_correlation.csv', index=False)

    # 打印相关性的前3名与最后3名
    top_3 = correlation_df.nlargest(3, 'correlation')
    bottom_3 = correlation_df.nsmallest(3, 'correlation')

    print("Top 3 correlated features:")
    for _, row in top_3.iterrows():
        print(f"Dimension: {row['dimension']}, Feature: {row['feature']}, Value: {row['value']}, Correlation: {row['correlation']}")

    print("\nBottom 3 correlated features:")
    for _, row in bottom_3.iterrows():
        print(f"Dimension: {row['dimension']}, Feature: {row['feature']}, Value: {row['value']}, Correlation: {row['correlation']}")

    # 打印维度列
    print("\nDimension columns:")
    for column in dimension_columns:
        print(column)


from scipy.stats import chi2_contingency

def getChiSquare(topVideoDf, videoParamsDf):
    df = pd.merge(topVideoDf, videoParamsDf, on='material_name', how='left')
    dimension_columns = topVideoDf.columns.difference(['material_name', 'is_top'])
    feature_columns = videoParamsDf.columns.difference(['material_name'])
    print('feature_columns:', feature_columns)
    
    chi_square_results = []
    grouped = df.groupby(list(dimension_columns))

    for name, group in grouped:
        for feature in feature_columns:
            contingency_table = pd.crosstab(group[feature], group['is_top'])
            chi2, p, _, _ = chi2_contingency(contingency_table)
            result = {
                'dimension': name,
                'feature': feature,
                'chi2': chi2,
                'p_value': p
            }
            chi_square_results.append(result)

    chi_square_df = pd.DataFrame(chi_square_results)
    chi_square_df.to_csv('/src/data/zk2/lw_chi_square_results.csv', index=False)

    top_3 = chi_square_df.nsmallest(3, 'p_value')
    bottom_3 = chi_square_df.nlargest(3, 'p_value')

    print("Top 3 chi-square features:")
    for _, row in top_3.iterrows():
        print(f"Dimension: {row['dimension']}, Feature: {row['feature']}, Chi2: {row['chi2']}, P-value: {row['p_value']}")

    print("\nBottom 3 chi-square features:")
    for _, row in bottom_3.iterrows():
        print(f"Dimension: {row['dimension']}, Feature: {row['feature']}, Chi2: {row['chi2']}, P-value: {row['p_value']}")

    print("\nDimension columns:")
    for column in dimension_columns:
        print(column)

from sklearn.feature_selection import mutual_info_classif

def getInformationGain(topVideoDf, videoParamsDf):
    df = pd.merge(topVideoDf, videoParamsDf, on='material_name', how='left')
    dimension_columns = topVideoDf.columns.difference(['material_name', 'is_top'])
    feature_columns = videoParamsDf.columns.difference(['material_name'])
    information_gain_results = []
    grouped = df.groupby(list(dimension_columns))

    for name, group in grouped:
        X = pd.get_dummies(group[feature_columns])
        y = group['is_top']
        mutual_info = mutual_info_classif(X, y, discrete_features=True)
        for feature, info_gain in zip(X.columns, mutual_info):
            result = {
                'dimension': name,
                'feature': feature,
                'information_gain': info_gain
            }
            information_gain_results.append(result)

    information_gain_df = pd.DataFrame(information_gain_results)
    information_gain_df.to_csv('/src/data/zk2/lw_information_gain_results.csv', index=False)

    top_3 = information_gain_df.nlargest(3, 'information_gain')
    bottom_3 = information_gain_df.nsmallest(3, 'information_gain')

    print("Top 3 information gain features:")
    for _, row in top_3.iterrows():
        print(f"Dimension: {row['dimension']}, Feature: {row['feature']}, Information Gain: {row['information_gain']}")

    print("\nBottom 3 information gain features:")
    for _, row in bottom_3.iterrows():
        print(f"Dimension: {row['dimension']}, Feature: {row['feature']}, Information Gain: {row['information_gain']}")

    print("\nDimension columns:")
    for column in dimension_columns:
        print(column)


if __name__ == "__main__":
    result_df = getTopVideoDataWithLabel(installTimeStart='20240601', installTimeEnd='20240630',dim='appPackage+mediasource', cost_threshold=0.05)
    # print(result_df)
    print(result_df[result_df['is_top'] == 1])

    p1Df = getVideoParams01(installTimeStart='20240601',installTimeEnd='20240630')
    # print(p1Df)

    # getCorrelation(result_df, p1Df)

    getChiSquare(result_df, p1Df)

    getInformationGain(result_df, p1Df)
