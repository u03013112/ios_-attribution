# 解决内存压力过大问题

import pandas as pd

# 目前压力来自下面代码

def test():
    userDf = pd.read_csv('/src/data/zk/userDf601.csv')
    print('初始状态')
    userDf.info(memory_usage='deep')
    # 类型优化
    userDf['day'] = userDf['day'].astype(pd.Int32Dtype())
    userDf['install_date'] = userDf['install_date'].astype(pd.StringDtype())
    for col in userDf.iloc[:, 3:].columns:
        userDf[col] = userDf[col].astype('float32')
    print('类型优化后')
    userDf.info(memory_usage='deep')

    # # 发现这样的行非常少，暂时不做
    # # 筛选行，去掉所有rate都是0的行
    # userDf = userDf[userDf.iloc[:, 3:].sum(axis=1) != 0]
    # print('筛选后')
    # userDf.info(memory_usage='deep')

    attDf_melted = userDf.melt(
            id_vars=['customer_user_id', 'install_date', 'day'],
            var_name='campaign_id',
            value_name='rate'
        )
    print('melt后')
    attDf_melted.info(memory_usage='deep')

if __name__ == '__main__':
    test()