# 市场偏好分析

# 主要针对不同国家的市场偏好进行分析，为了解释为什么lastwar在US表现与top3差异较大（少了大概70%）
# 参考资料 台湾 lastwar与top3基本持平
# 可能需要进一步将国家拆开，目前的国家分组数据样本太少

# 找到和台湾市场偏好类似的国家，以及和美国市场偏好类似的国家
# 找到lastwar与top3表现差异类似的 和台湾类似的 以及和美国类似的国家
# 对比上面两个分组，是否有一定关联性

# 市场偏好类似分析

# 获取每个国家的top N游戏，然后针对 top N的游戏进行 相似分类
# 这个方案有局限，很可能得不到好的结果，因为只有top3，相似程度只有100% 66.67% 33.33% 和0%这么几种简单的情况
# 而且存在一些国家或地区有特殊情况，单独发布版本，游戏的名字不一样，但是实际上是同一个游戏
# 可以考虑用st的那个同一游戏名，再夺取一些top N的游戏，然后再进行相似分类

# 或者针对top N 的游戏类型进行 相似分类，分类可能也要参考st的数据，看看st是否可以提供这个数据


# 有关《MONOPOLY GO!》 和类似涉及到 赌博、博彩 或类似分类的产品
# 这可能需要排除在市场偏好分析之外，这类产品与lastwar的差异过大，收入金额比较价值不大

# 发现在落后国家，收入lastwar与top3收入差距逐渐变大，可能是游戏类型影响，所以上面的游戏类型分析可能有帮助
import os
import requests
import pandas as pd

import json

import sys
sys.path.append('/src')

from src.config import sensortowerToken

from top3 import getDataFromSt,getRevenueTop3,getlastwarRevenue,lwAppId

# 获得美国各个分类的游戏排名的前100个
# 用来映射游戏类型
# 如果没有匹配到找到的这些top100游戏，那么就认为是其他类型游戏。
def getGameCategoryFromSt(category='7001',limit=100,date='2024-01-01',end_date='2024-06-30'):
    filename = f'/src/data/zk2/gameTypeFromSt_{category}_{date}_{end_date}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # https://api.sensortower-china.com/v1/unified/sales_report_estimates_comparison_attributes?comparison_attribute=absolute&time_range=quarter&measure=revenue&device_type=total&category=7002&date=2024-04-01&end_date=2024-06-30&regions=US&limit=25&custom_tags_mode=exclude_unified_apps&auth_token=YOUR_AUTHENTICATION_TOKEN
        url = f'https://api.sensortower-china.com/v1/unified/sales_report_estimates_comparison_attributes?comparison_attribute=absolute&time_range=quarter&measure=revenue&device_type=total&category={category}&date={date}&end_date={end_date}&regions=US&limit={limit}&custom_tags_mode=exclude_unified_apps&auth_token={sensortowerToken}'

        r = requests.get(url)

        if r.status_code != 200:
            print('Error: getTopApp failed, status_code:',r.status_code)
            print(r.text)
            return None

        addIds = []
        ret = r.json()
        for item in ret:
            appId = item['app_id']
            addIds.append(appId)

        df = pd.DataFrame(addIds,columns=['app_id'])
        df.to_csv(filename,index=False)

    return df

def getGameCategoryFromStAll():
    df = pd.DataFrame()
    categoryList = [
        {'name':'Action','category':'7001'},
        {'name':'Adventure','category':'7002'},
        {'name':'Casual','category':'7003'},
        {'name':'Board','category':'7004'},
        {'name':'Card','category':'7005'},
        {'name':'Casino','category':'7006'},
        {'name':'Family','category':'7009'},
        {'name':'Music','category':'7011'},
        {'name':'Puzzle','category':'7012'},
        {'name':'Racing','category':'7013'},
        {'name':'Role Playing','category':'7014'},
        {'name':'Simulation','category':'7015'},
        {'name':'Sports','category':'7016'},
        {'name':'Strategy','category':'7017'},
        {'name':'Trivia','category':'7018'},
        {'name':'Word','category':'7019'}
    ]
    for category in categoryList:
        categoryId = category['category']
        df2 = getGameCategoryFromSt(categoryId)
        df2['category'] = category['name']
        df = pd.concat([df,df2],ignore_index=True)
    return df

def getCountryGroupList():
    countryGroupList = [
        {'name':'T1', 'countries':['AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','GB','MO','IL']},
        {'name':'T2', 'countries':['BG','BV','BY','EE','ES','GL','GR','HU','ID','KZ','LT','LV','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA']},
        {'name':'T3', 'countries':['AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','HU','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','RS','SM','SR','UA','UY','XK']},
        {'name':'GCC', 'countries':['SA','AE','QA','KW','BH','OM']},
        {'name':'US', 'countries':['US']},
        {'name':'JP', 'countries':['JP']},
        {'name':'KR', 'countries':['KR']},
        {'name':'TW', 'countries':['TW']}
    ]
    return countryGroupList


# 获取每个国家收入的前三个游戏
def getRevenueTopN(df,countryGroupList = None,n=3):
    if countryGroupList is None:
        df['countryGroup'] = df['country']
    else:
        df['countryGroup'] = 'Others'
        # countryGroupList = getCountryGroupList()
        for countryGroup in countryGroupList:
            for country in countryGroup['countries']:
                df.loc[df['country'] == country, 'countryGroup'] = countryGroup['name']

    # for debug
    # print(df[df['countryGroup'] == 'Others'])

    df = df.groupby(['countryGroup','app_id']).agg(
        {
            # 'units':'sum',
            'revenue':'sum'
        }
    ).reset_index()

    # 不包含lastwar
    df = df[df['app_id'] != lwAppId]
        
    # print('debug countryGroup:',df['countryGroup'].unique())

    top3 = df.groupby('countryGroup').apply(
        lambda x: x.nlargest(n, 'revenue')
    ).reset_index(drop=True)
    
    return top3



from sklearn.preprocessing import OneHotEncoder
from sklearn.cluster import KMeans
# 市场分析之按照top3的app id来做相似分类
# 初步效果不好，没有分析出有说服力的结论
# 按照app_id进行分组，US和JP、KR、TW分组不同
# 分更多类，按照分类计算每个分类的top3游戏的平均收入，以及lastwar的收入，差距都很大
def marketAnalysisByAppId():
    sdData = getDataFromSt()
    countryGroupList = getCountryGroupList()
    top3Df = getRevenueTopN(sdData,None, n = 3)

    top3Df = top3Df[['countryGroup','app_id']]
    # print(top3Df)

    # 对 app_id 进行 one-hot 编码
    encoder = OneHotEncoder()
    app_id_encoded = encoder.fit_transform(top3Df[['app_id']]).toarray()

    # 获取 one-hot 编码后的列名
    app_id_columns = encoder.get_feature_names(['app_id'])

    # 将 one-hot 编码后的特征与 countryGroup 合并
    top3Df_encoded = pd.concat([top3Df[['countryGroup']], pd.DataFrame(app_id_encoded, columns=app_id_columns)], axis=1)
    # 按照 countryGroup 进行聚合，将每个 countryGroup 的 one-hot 编码合并成一行
    top3Df_grouped = top3Df_encoded.groupby('countryGroup').max().reset_index()

    # print(top3Df_grouped)

    # 对 countryGroup 进行 K-means 聚类，分成2组
    kmeans = KMeans(n_clusters=16, random_state=0)
    top3Df_grouped['cluster'] = kmeans.fit_predict(top3Df_grouped.drop('countryGroup', axis=1))

    # 打印分组结果
    # print(top3Df_grouped[['countryGroup', 'cluster']])
    print(top3Df_grouped.groupby('cluster').agg({'countryGroup': 'unique'}))

    us_cluster = top3Df_grouped[top3Df_grouped['countryGroup'] == 'US']['cluster'].values[0]
    # # 打印 US 所在的分类和该分类中的所有国家
    print(f"US 所在的分类: {us_cluster}")
    # countries_in_us_cluster = top3Df_grouped[top3Df_grouped['cluster'] == us_cluster]['countryGroup'].tolist()
    # print(f"分类 {us_cluster} 中的所有国家: {countries_in_us_cluster}")

    # 与 sdData 进行合并
    top3Df_grouped['country'] = top3Df_grouped['countryGroup']
    merged_data = sdData.merge(top3Df_grouped[['country', 'cluster']], on='country', how='left')

    # 计算每个分组和 app_id 的收入总和
    revenue_sum = merged_data.groupby(['cluster', 'app_id'])['revenue'].sum().reset_index()

    # 计算每个分组的 app_id 收入前3名的平均值
    top3_revenue_avg = revenue_sum.groupby('cluster').apply(lambda x: x.nlargest(3, 'revenue')['revenue'].mean()).reset_index(name='top3_avg_revenue')

    # 计算每个分组中特定 app_id 的收入总和
    lwAppId = '64075e77537c41636a8e1c58'
    lw_revenue_sum = merged_data[merged_data['app_id'] == lwAppId].groupby('cluster')['revenue'].sum().reset_index(name='lw_revenue_sum')

    # 合并结果
    result = top3_revenue_avg.merge(lw_revenue_sum, on='cluster', how='left')

    # 计算每个分组的 lw 收入与前3名收入平均值的比值
    result['lw_to_top3_ratio'] = result['lw_revenue_sum'] / result['top3_avg_revenue']

    # 打印结果
    print(result)
    

def getGameCategoryDf():
    # 读取所有游戏分类数据
    gameCategoryData = getGameCategoryFromStAll()

    # 对分类进行 One-hot 编码
    encoder = OneHotEncoder(sparse=False)
    category_encoded = encoder.fit_transform(gameCategoryData[['category']])

    # 获取 One-hot 编码后的列名
    category_columns = encoder.get_feature_names(['category'])

    # 将 One-hot 编码后的特征与原始数据合并
    gameCategoryData_encoded = pd.concat([gameCategoryData[['app_id']], pd.DataFrame(category_encoded, columns=category_columns)], axis=1)

    # 针对 app_id 汇总 One-hot 结果
    gameCategoryData_aggregated = gameCategoryData_encoded.groupby('app_id').sum().reset_index()

    return gameCategoryData_aggregated

# 按国家分组分析
def marketAnalysisByAppCategory():
    sdData = getDataFromSt()
    sdData = sdData[sdData['country'] != 'CN']
    # 找到每个国家的 top3 游戏
    # top3Df = getRevenueTopN(sdData, n=3)
    top3Df = getRevenueTopN(sdData,countryGroupList=getCountryGroupList(), n=3)

    top3Df.rename(columns={'countryGroup':'country'}, inplace=True)

    gameCategoryData = getGameCategoryDf()
    merged_data = top3Df.merge(gameCategoryData, on='app_id', how='left')

    # print(merged_data.head())

    category_columns = [col for col in gameCategoryData.columns if col != 'app_id']
    merged_data_encoded = merged_data[['country'] + category_columns]
    merged_data_grouped = merged_data_encoded.groupby('country').sum().reset_index()

    # 对 country 进行 K-means 聚类，分成4组
    kmeans = KMeans(n_clusters=4, random_state=0)
    merged_data_grouped['cluster'] = kmeans.fit_predict(merged_data_grouped.drop('country', axis=1))

    # 打印 US 所在的分类和该分类中的所有国家
    print(f"US 所在的分类: {merged_data_grouped[merged_data_grouped['country'] == 'US']['cluster'].values[0]}")
    print(f'KR 所在的分类: {merged_data_grouped[merged_data_grouped["country"] == "KR"]["cluster"].values[0]}')

    print(merged_data_grouped.groupby('cluster').agg({'country': 'unique'}))

    # 提取质心
    centroids = kmeans.cluster_centers_

    # 构建质心数据框
    centroid_df = pd.DataFrame(centroids, columns=category_columns)
    centroid_df['cluster'] = centroid_df.index

    # 获取每个分类的国家列表
    cluster_countries = merged_data_grouped.groupby('cluster')['country'].apply(list).reset_index()
    centroid_df = centroid_df.merge(cluster_countries, on='cluster', how='left')

    # 保留质心数值的小数点后两位
    centroid_df[category_columns] = centroid_df[category_columns].round(2)

    # 保存质心和国家列表到 CSV 文件
    centroid_df.to_csv('/src/data/k1.csv', index=False)

    sdData['countryGroup'] = 'Others'
    for countryGroup in getCountryGroupList():
        for country in countryGroup['countries']:
            sdData.loc[sdData['country'] == country, 'countryGroup'] = countryGroup['name']

    sdData['country'] = sdData['countryGroup']

    merged_data = sdData.merge(merged_data_grouped[['country', 'cluster']], on='country', how='left')
    # 计算每个分组和 app_id 的收入总和
    revenue_sum = merged_data.groupby(['cluster', 'app_id'])['revenue'].sum().reset_index()
    # 计算每个分组的 app_id 收入前3名的平均值
    top3_revenue_avg = revenue_sum.groupby('cluster').apply(lambda x: x.nlargest(3, 'revenue')['revenue'].mean()).reset_index(name='top3_avg_revenue')

    # 计算每个分组中特定 app_id 的收入总和
    lwAppId = '64075e77537c41636a8e1c58'
    lw_revenue_sum = merged_data[merged_data['app_id'] == lwAppId].groupby('cluster')['revenue'].sum().reset_index(name='lw_revenue_sum')

    # 合并结果
    result = top3_revenue_avg.merge(lw_revenue_sum, on='cluster', how='left')

    result['lw/top3'] = result['lw_revenue_sum'] / result['top3_avg_revenue']
    
    # 打印结果
    print(result)

# 拆开国家分析
def marketAnalysisByAppCategory2():
    sdData = getDataFromSt()
    sdData = sdData[sdData['country'] != 'CN']
    # 找到每个国家的 top3 游戏
    top3Df = getRevenueTopN(sdData, n=3)
    # top3Df = getRevenueTopN(sdData,countryGroupList=getCountryGroupList(), n=3)

    top3Df.rename(columns={'countryGroup':'country'}, inplace=True)

    gameCategoryData = getGameCategoryDf()
    merged_data = top3Df.merge(gameCategoryData, on='app_id', how='left')

    # print(merged_data.head())

    category_columns = [col for col in gameCategoryData.columns if col != 'app_id']
    merged_data_encoded = merged_data[['country'] + category_columns]
    merged_data_grouped = merged_data_encoded.groupby('country').sum().reset_index()

    # 对 country 进行 K-means 聚类，分成4组
    kmeans = KMeans(n_clusters=8, random_state=0)
    merged_data_grouped['cluster'] = kmeans.fit_predict(merged_data_grouped.drop('country', axis=1))

    # 打印 US 所在的分类和该分类中的所有国家
    print(f"US 所在的分类: {merged_data_grouped[merged_data_grouped['country'] == 'US']['cluster'].values[0]}")
    print(f'KR 所在的分类: {merged_data_grouped[merged_data_grouped["country"] == "KR"]["cluster"].values[0]}')
    r = merged_data_grouped.groupby('cluster').agg({'country': 'unique'})
    pd.set_option('display.max_colwidth', None) 
    print(r)

    # 提取质心
    centroids = kmeans.cluster_centers_

    # 构建质心数据框
    centroid_df = pd.DataFrame(centroids, columns=category_columns)
    centroid_df['cluster'] = centroid_df.index

    # 获取每个分类的国家列表
    cluster_countries = merged_data_grouped.groupby('cluster')['country'].apply(list).reset_index()
    centroid_df = centroid_df.merge(cluster_countries, on='cluster', how='left')

    # 保留质心数值的小数点后两位
    centroid_df[category_columns] = centroid_df[category_columns].round(2)


    # 保存质心和国家列表到 CSV 文件
    centroid_df.to_csv('/src/data/k2.csv', index=False)

    merged_data = sdData.merge(merged_data_grouped[['country', 'cluster']], on='country', how='left')
    # 计算每个分组和 app_id 的收入总和
    revenue_sum = merged_data.groupby(['cluster', 'app_id'])['revenue'].sum().reset_index()
    # 计算每个分组的 app_id 收入前3名的平均值
    top3_revenue_avg = revenue_sum.groupby('cluster').apply(lambda x: x.nlargest(3, 'revenue')['revenue'].mean()).reset_index(name='top3_avg_revenue')

    # 计算每个分组中特定 app_id 的收入总和
    lwAppId = '64075e77537c41636a8e1c58'
    lw_revenue_sum = merged_data[merged_data['app_id'] == lwAppId].groupby('cluster')['revenue'].sum().reset_index(name='lw_revenue_sum')

    # 合并结果
    result = top3_revenue_avg.merge(lw_revenue_sum, on='cluster', how='left')

    result['lw/top3'] = result['lw_revenue_sum'] / result['top3_avg_revenue']
    
    # 打印结果
    print(result)


def top3WithCategory(withCategoryList = [],withoutCategoryList = []):
    # 设置浮点数显示格式
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

    sdData = getDataFromSt()
    print('sdData len:',len(sdData))
    gameCategoryData = getGameCategoryDf()
    sdData = sdData.merge(gameCategoryData, on='app_id', how='left')

    # 初始化筛选条件为全为 True
    condition = pd.Series(True, index=sdData.index)

    if len(withCategoryList) > 0:
        for category in withCategoryList:
            condition = condition & (sdData['category_'+category] > 0)
        
    if len(withoutCategoryList) > 0:
        for category in withoutCategoryList:
            condition = condition & (sdData['category_'+category] == 0)

    sdData = sdData[condition]
    print('sdData len:',len(sdData))

    
    top3Df = getRevenueTop3(sdData)
    top3Df = top3Df.groupby(['countryGroup']).agg(
        {
            # 'units':'mean',
            'revenue':'mean'
        }
    ).reset_index()
    top3Df = top3Df[['countryGroup','revenue']]
    # print(top3Df)

    lwData = getlastwarRevenue(sdData)
    lwData = lwData[['countryGroup','revenue']]
    # print(lwData)

    mergeDf = pd.merge(top3Df, lwData, on='countryGroup', how='left', suffixes=('_top3', '_lw'))
    mergeDf['lw/top3'] = mergeDf['revenue_lw'] / mergeDf['revenue_top3']

    mergeDf['lw/top3'] = mergeDf['lw/top3'].apply(lambda x: '%.2f%%'%(x*100))
    print(mergeDf)


# 只计算SLG游戏
# curl -X 'GET' \
#   'https://api.sensortower-china.com/v1/unified/sales_report_estimates_comparison_attributes?comparison_attribute=absolute&time_range=year&measure=revenue&device_type=total&category=6014&date=2024-01-01&end_date=2024-06-30&regions=US&limit=100&custom_fields_filter_id=600a22c0241bc16eb899fd71&custom_tags_mode=include_unified_apps&auth_token=YOUR_AUTHENTICATION_TOKEN' \
#   -H 'accept: application/json'
def top3WithCategorySLG():
    # 设置浮点数显示格式
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

    sdData = getDataFromSt()
    gameCategoryData = getGameCategoryDf()
    sdData = sdData.merge(gameCategoryData, on='app_id', how='left')

    # Q2
    # filename = 'response_1723170889860.json'
    # Q1 & Q2
    filename = 'response_1723448192124.json'

    with open(filename) as f:
        slgJsonStr = f.read()

    slgAppIdList = []
    slgJson = json.loads(slgJsonStr)
    for j in slgJson:
        app_id = j['app_id']
        slgAppIdList.append(app_id)

    print('slgAppIdList:',slgAppIdList)

    sdData['isSLG'] = False
    sdData.loc[sdData['app_id'].isin(slgAppIdList), 'isSLG'] = True

    sdData = sdData[sdData['isSLG']]

    
    top3Df = getRevenueTop3(sdData)
    top3Df = top3Df.groupby(['countryGroup']).agg(
        {
            # 'units':'mean',
            'revenue':'mean'
        }
    ).reset_index()
    top3Df = top3Df[['countryGroup','revenue']]
    # print(top3Df)

    lwData = getlastwarRevenue(sdData)
    lwData = lwData[['countryGroup','revenue']]
    # print(lwData)

    mergeDf = pd.merge(top3Df, lwData, on='countryGroup', how='left', suffixes=('_top3', '_lw'))
    mergeDf['lw/top3'] = mergeDf['revenue_lw'] / mergeDf['revenue_top3']

    mergeDf['lw/top3'] = mergeDf['lw/top3'].apply(lambda x: '%.2f%%'%(x*100))
    print(mergeDf)


if __name__ == "__main__":
    # marketAnalysisByAppId()

    # marketAnalysisByAppCategory()

    # marketAnalysisByAppCategory2()

    # top3WithCategory(withCategoryList=[])
    # top3WithCategory(withCategoryList=['Strategy'])
    # top3WithCategory(withoutCategoryList=['Casual','Family'])
    # top3WithCategory(withoutCategoryList=['Role Playing'])

    top3WithCategorySLG()
    

    
