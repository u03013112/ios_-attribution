# 用最简单方式进行拟合并验算
import pandas as pd
import numpy as np
from getData import getRawData,getGroupData,getNerfRData


# 计算7日收入与3日收入的比值
def r7r3(df):
    # 将参数中的df中的，列：users_count，total_revenue_d1 进行舍弃
    # 将除 total_revenue_d3、total_revenue_d7 的列作为 groupby 的依据
    # groupby之后，每一行计算一个比值
    # 最后将比值汇总求平均
    # 最终输出列：groupby的依据列（排除掉install_day），r7r3
    
    pass

def main():
    rawDf0, rawDf1, rawDf2 = getRawData()
    