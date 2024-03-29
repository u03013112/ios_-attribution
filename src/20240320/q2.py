# 对于目前lw项目融合归因fb偏低的分析

# 或者更加广泛的，对于某一时间段，某一个或者多个媒体的表现分析

# 大致思路
# 1. 找到skan中的cv分布，分时间，分媒体
# 2. 找到分配失败的比例，包括人数比例与金额比例
# 3. 对于各个级别的cv，统计这个媒体中的 归因 个体 与整体（大盘）的表现差异
# 基于上面分析，可能可以得到一些基础结论。比如某媒体在一段时间表现为什么差，要先看SKAN是否差，然后再看他的cv分布中，哪部分的表现差于大盘，或者哪个部分比例低于大盘
# 对于分配失败部分的补充，暂时没有太好想法。

# 输入参数
# 1. 时间段，开始时间，结束时间


# 步骤
# 1. 获得skan数据，要根据输入时间段，适当的扩大范围，确保这个时间段的融合归因是完整的
# 2. 对这段skan数据进行融合归因，除了基础逻辑外，还要在过程中添加额外统计
# 2.1 指定时间段的分配失败数据，skan条目，要逐条记录

# 计算这段时间大盘的cv分布（使用BI数据）
# 计算这段时间每个媒体的cv分布（使用SKAN数据）,这里考虑要出完全版本与分配成功部分版本
# 计算这段时间每个媒体的每个归因分配用户的长期回收，比如7日回收。再根据cv分组，计算每个媒体，每个cv的付费增长率
# 计算大盘的各cv的付费增长率
# 对付费增长率进行比较

# 得到结论可能有，cv结构不同，导致付费增长率不同，导致感觉某媒体表现差
# 或是这个媒体的cv增长率不如大盘，导致表现差。这部分要尝试抽样跟踪，争取找到有力的证据证明这个结论。
# 还有可能是skan首日表现就不好。或者分配失败的部分，导致表现差。这部分暂时没有太好的解决方案。


# 暂时的疑惑或者可能的隐患
# 融合归因是链式的算法，单独对着一段进行融合归因，与目前线上融合归因结论可能有一定的偏差。这个偏差可能会导致结论不准确。
# 是否直接使用线上版本结论，来做上面分析。
# 这个可能也是简单的思路。

# 这是个思路，可以直接用结论来反向的获得数据，而不是再做一次融合归因。
# 但是怎么计算失败的分配，这个还是需要融合归因的数据。

# 所以是不是应该分为2个问题
# 将分配失败的问题单独拿出来，而不是混在一起。

# 分配失败的问题再考虑一下怎么解决
# 目前的结果是分配失败的情况比较多，金额上差了15%左右。
# 这可能使某些媒体的cv分布产生偏差。
# 这部分最好可以有效的评估一下，影响的程度。
# 可以修改线上代码，在融合归因的时候，将分配失败的部分，单独记录下来。



# TODO:融合归因目前存在问题，即用户分配比例，目前是分配超过95%就不再分配，但其实这是不理想的。应该改为在这个用户所有的已分配列表中排序，找到这条skan是否更加可信，可以替代目前的分配。但是这个效率太低了，需要考虑算法。
# 可能需要区别对待，对待付费用户可以采用这种高消耗的算法，对待非付费用户，可以采用目前的算法。
# 然后再将结果合在一起。

# 重新规划一下思路
# 先不考虑分配失败的问题，直接使用线上的融合归因结论，来做分析。
# 将融合归因结论，与媒体，24小时付费，7日付费关联，做出一张表，列media，cv，人数，24小时付费，7日付费，可以再加上一个campaign



# 预计结论应该是，高cv的付费张张能力更好。不同媒体间的同cv付费能力差不多。主要差异体现在不同媒体的cv分布上。



import pandas as pd

def debug():
    df = pd.read_csv('/src/data/zk2/userDfRet0322.csv')
    # 选择列名以'rate'结尾的列
    rate_cols = df.filter(regex='rate$')

    # 计算所有数据的和
    total = rate_cols.sum().sum()

    print(total)
    return total

def debug2():
    df = pd.read_csv('/src/data/zk2/userDfRet0322.csv')

    # df['install_date'] 是类似2024-02-06 12:00:00的字符串，取日期部分
    df['install_date'] = df['install_date'].str.split(' ').str[0]
    install_date = df['install_date'].unique().tolist()
    install_date.sort()

    print(install_date)

if __name__ == '__main__':
    debug2()