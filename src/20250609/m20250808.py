
import datetime
import pandas as pd

import sys

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO

# 针对20250806_20进行修改：
# 小媒体系数最大值1.25
# Facebook T1 1.4左右
# Facebook KR 1
# Facebook other 1.25 
# 写入tag = 20250808_20

def m20250808():
    """手动写入贝叶斯结果数据到ODPS"""
    try:
        # 获取ODPS连接
        o = getO()
        table = o.get_table('lw_20250703_ios_bayesian_result_by_j')
        
        tag = "20250808_20"
        print(f"处理分区: {tag}")
        
        # 准备写入的数据
        write_data = [
            {
                'country_group': 'GCC',
                'organic_revenue': 823.5,
                'applovin_int_d7_coeff': 1.0,
                'applovin_int_d28_coeff': 1.0,
                'facebook_ads_coeff': 1.122,
                'moloco_int_coeff': 1.106,
                'bytedanceglobal_int_coeff': 1.25
            },
            {
                'country_group': 'JP',
                'organic_revenue': 1405.0,
                'applovin_int_d7_coeff': 1.0,
                'applovin_int_d28_coeff': 1.0,
                'facebook_ads_coeff': 1.146,
                'moloco_int_coeff': 1.202,
                'bytedanceglobal_int_coeff': 0.939
            },
            {
                'country_group': 'KR',
                'organic_revenue': 848.4,
                'applovin_int_d7_coeff': 1.0,
                'applovin_int_d28_coeff': 1.0,
                'facebook_ads_coeff': 1.004,
                'moloco_int_coeff': 1.088,
                'bytedanceglobal_int_coeff': 1.059
            },
            {
                'country_group': 'T1',
                'organic_revenue': 4094.0,
                'applovin_int_d7_coeff': 1.0,
                'applovin_int_d28_coeff': 1.0,
                'facebook_ads_coeff': 1.401,
                'moloco_int_coeff': 0.978,
                'bytedanceglobal_int_coeff': 1.25
            },
            {
                'country_group': 'US',
                'organic_revenue': 4504.0,
                'applovin_int_d7_coeff': 1.0,
                'applovin_int_d28_coeff': 1.0,
                'facebook_ads_coeff': 1.263,
                'moloco_int_coeff': 1.105,
                'bytedanceglobal_int_coeff': 1.25
            },
            {
                'country_group': 'other',
                'organic_revenue': 2725.0,
                'applovin_int_d7_coeff': 1.0,
                'applovin_int_d28_coeff': 1.0,
                'facebook_ads_coeff': 1.25,
                'moloco_int_coeff': 1.25,
                'bytedanceglobal_int_coeff': 1.25
            }
        ]
        
        # 转换为DataFrame
        write_df = pd.DataFrame(write_data)
        
        # 删除已存在的分区（如果存在）
        try:
            table.delete_partition(f"tag='{tag}'", if_exists=True)
            print(f"已删除分区: tag='{tag}'")
        except Exception as e:
            print(f"删除分区失败（可能不存在）: {e}")
        
        # 创建新分区
        try:
            table.create_partition(f"tag='{tag}'", if_not_exists=True)
            print(f"已创建分区: tag='{tag}'")
        except Exception as e:
            print(f"创建分区失败: {e}")
            return
        
        # 写入数据到分区
        try:
            with table.open_writer(partition=f"tag='{tag}'", arrow=True) as writer:
                writer.write(write_df)
            
            print(f"成功写入 {len(write_df)} 条记录到分区 tag='{tag}'")
            print(f"分区 {tag} 数据预览:")
            print(write_df)
            print("-" * 50)
            
        except Exception as e:
            print(f"写入分区 {tag} 失败: {e}")
            # 保存到本地作为备份
            backup_filename = f'/src/data/odps_backup_{tag}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            write_df.to_csv(backup_filename, index=False)
            print(f"分区 {tag} 数据已备份到: {backup_filename}")
        
        print(f"分区 {tag} 写入完成！")
        
    except Exception as e:
        print(f"写入ODPS失败: {e}")
        # 保存到本地作为备份
        backup_filename = f'/src/data/odps_backup_{tag}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        pd.DataFrame(write_data).to_csv(backup_filename, index=False)
        print(f"数据已备份到: {backup_filename}")


if __name__ == "__main__":
    m20250808()