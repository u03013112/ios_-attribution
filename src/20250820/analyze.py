# 分析数据

import pandas as pd
import numpy as np
from getData import getRawData


def analyze_raw_data():
    # 获取原始数据（按国家分组）
    rawDf0, rawDf1, rawDf2 = getRawData()
    
    # 分国家相关性分析
    # 计算每个国家的3日收入和7日收入之间的相关性
    correlation_results = []
    
    for country in rawDf0['country_group'].unique():
        country_data = rawDf0[rawDf0['country_group'] == country]
        
        # 确保有足够的数据点进行相关性计算
        if len(country_data) > 1:
            # 计算3日收入和7日收入的相关系数
            corr = country_data['total_revenue_d3'].corr(country_data['total_revenue_d7'])
            
            # 如果相关系数为NaN（比如所有值都相同），设为0
            if pd.isna(corr):
                corr = 0.0
                
            correlation_results.append({
                'country_group': country,
                'corr_between_r3_r7': corr
            })
        else:
            # 数据点不足，设相关系数为0
            correlation_results.append({
                'country_group': country,
                'corr_between_r3_r7': 0.0
            })
    
    # 转换为DataFrame
    result_df = pd.DataFrame(correlation_results)
    
    # 按相关系数降序排列
    result_df = result_df.sort_values('corr_between_r3_r7', ascending=False).reset_index(drop=True)
    
    # 保存CSV文件
    filename = f'/src/data/20250820_raw_corr0.csv'
    result_df.to_csv(filename, index=False)
    print(f"相关性分析结果已保存到: {filename}")
    
    # 打印结果
    print("分国家3日收入与7日收入相关性分析:")
    print(result_df)

    # 分国家+分媒体相关性分析
    # 比上面多加一列media
    correlation_results_media = []
    
    for country in rawDf1['country_group'].unique():
        for media in rawDf1[rawDf1['country_group'] == country]['mediasource'].unique():
            country_media_data = rawDf1[(rawDf1['country_group'] == country) & (rawDf1['mediasource'] == media)]
            
            # 确保有足够的数据点进行相关性计算
            if len(country_media_data) > 1:
                # 计算3日收入和7日收入的相关系数
                corr = country_media_data['total_revenue_d3'].corr(country_media_data['total_revenue_d7'])
                
                # 如果相关系数为NaN（比如所有值都相同），设为0
                if pd.isna(corr):
                    corr = 0.0
                    
                correlation_results_media.append({
                    'country_group': country,
                    'mediasource': media,
                    'corr_between_r3_r7': corr
                })
            else:
                # 数据点不足，设相关系数为0
                correlation_results_media.append({
                    'country_group': country,
                    'mediasource': media,
                    'corr_between_r3_r7': 0.0
                })
    
    # 转换为DataFrame
    result_media_df = pd.DataFrame(correlation_results_media)
    
    # 按相关系数降序排列
    result_media_df = result_media_df.sort_values('corr_between_r3_r7', ascending=False).reset_index(drop=True)
    
    # 保存CSV文件
    filename_media = f'/src/data/20250820_raw_corr1.csv'
    result_media_df.to_csv(filename_media, index=False)
    print(f"分国家+分媒体相关性分析结果已保存到: {filename_media}")
    
    # 打印结果
    print("分国家+分媒体3日收入与7日收入相关性分析:")
    print(result_media_df.head(10))  # 只显示前10行

    # 分媒体+分campaign相关性分析，不再需要分国家
    # 因为campaign可能有很多，所以不止要有media和campaign_id，还需要有users_count，让我可以快速的知道相关性低的campaign是不是都是用户少的
    correlation_results_campaign = []
    
    for media in rawDf2['mediasource'].unique():
        for campaign in rawDf2[rawDf2['mediasource'] == media]['campaign_id'].unique():
            media_campaign_data = rawDf2[(rawDf2['mediasource'] == media) & (rawDf2['campaign_id'] == campaign)]
            
            # 计算总用户数
            total_users = media_campaign_data['users_count'].sum()
            
            # 确保有足够的数据点进行相关性计算
            if len(media_campaign_data) > 1:
                # 计算3日收入和7日收入的相关系数
                corr = media_campaign_data['total_revenue_d3'].corr(media_campaign_data['total_revenue_d7'])
                
                # 如果相关系数为NaN（比如所有值都相同），设为0
                if pd.isna(corr):
                    corr = 0.0
                    
                correlation_results_campaign.append({
                    'mediasource': media,
                    'campaign_id': campaign,
                    'users_count': total_users,
                    'corr_between_r3_r7': corr
                })
            else:
                # 数据点不足，设相关系数为0
                correlation_results_campaign.append({
                    'mediasource': media,
                    'campaign_id': campaign,
                    'users_count': total_users,
                    'corr_between_r3_r7': 0.0
                })
    
    # 转换为DataFrame
    result_campaign_df = pd.DataFrame(correlation_results_campaign)
    
    # 按相关系数降序排列
    result_campaign_df = result_campaign_df.sort_values('corr_between_r3_r7', ascending=False).reset_index(drop=True)
    
    # 保存CSV文件
    filename_campaign = f'/src/data/20250820_raw_corr2.csv'
    result_campaign_df.to_csv(filename_campaign, index=False)
    print(f"分媒体+分campaign相关性分析结果已保存到: {filename_campaign}")
    
    # 打印结果
    print("分媒体+分campaign3日收入与7日收入相关性分析:")
    print(result_campaign_df.head(10))  # 只显示前10行
    
    return result_df


if __name__ == "__main__":
    analyze_raw_data()
