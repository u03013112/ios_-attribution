import pandas as pd

import sys
sys.path.append('/src')

def getFilename(filename,directory,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)

# 获得自然量占比的较大值与较小值
# 按照国家，统计自然量占比的最大值与最小值
# 目前的简单做法是直接获取上一周期和本周期数据，将较大的值作为最大值，较小的值作为最小值
def getOrganicRateMaxMin(directory):
    df = pd.read_csv(getFilename('reportOrganic2',directory,'csv'))
    c1 = df.columns[2]
    c2 = df.columns[3]
    # 将原本的类似 0.1% 的数据转换成 0.001
    df[c1] = df[c1].apply(lambda x:float(x[:-1])/100)
    df[c2] = df[c2].apply(lambda x:float(x[:-1])/100)

    df['organic_rate_max'] = df[[c1,c2]].max(axis=1)
    df['organic_rate_min'] = df[[c1,c2]].min(axis=1)
    df = df[['国家','organic_rate_max','organic_rate_min']]
    return df

# KPI指标
# TODO：这个KPI和外面的存在多次定义，需要统一来源
kpi = {
    'US':0.065,
    'KR':0.065,
    'JP':0.055,
    'GCC':0.06,
    'other':0.07
}

# 用标准KPI和自然量占比，推算出不含自然量的KPI的最大值与最小值
# 基础公式：KPIMax = KPI*（1-自然量占比最小值）；KPIMin = KPI*（1-自然量占比最大值）
def getKpiMaxMin(directory):
    kpiDf = pd.DataFrame(list(kpi.items()), columns=['国家', 'KPI'])
    df = getOrganicRateMaxMin(directory)

    df = pd.merge(df,kpiDf,on='国家',how='left')

    df['kpi_max'] = df['KPI'] * (1 - df['organic_rate_min'])
    df['kpi_min'] = df['KPI'] * (1 - df['organic_rate_max'])
    print(df)
    df = df[['国家','kpi_max','kpi_min']]

    return df

def text2Fix(directory):
    kpiMaxMin = getKpiMaxMin(directory)

    mediaList = ['bytedanceglobal','facebook','google']
    for media in mediaList:
        mediaDf = pd.read_csv(getFilename(f'report2_{media}',directory,'csv'))

        mediaDfCopy = mediaDf.copy()

        mediaDf['ROI7D环比'] = mediaDf['ROI7D环比'].apply(lambda x:float(x[:-1])/100)
        mediaDf['cost环比'] = mediaDf['cost环比'].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,1] = mediaDf.iloc[:,1].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,2] = mediaDf.iloc[:,2].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,8] = mediaDf.iloc[:,8].apply(lambda x:float(x[:-1])/100)
        mediaDf.iloc[:,9] = mediaDf.iloc[:,9].apply(lambda x:float(x[:-1])/100)
        
        mediaDf = mediaDf.merge(kpiMaxMin,on='国家',how='left')

        ret1 = ''
        ret2 = ''

        # 获得mediaDf中列kpi_min的列索引
        kpi_min_index = mediaDf.columns.get_loc('kpi_min')

        print(mediaDf)
        for i in range(len(mediaDf)):
            if mediaDf.iloc[i,0] == '所有国家汇总':
                continue
            costOp = '下降' if mediaDf.iloc[i,7] > 0 else '上升'
            if mediaDf.iloc[i,1] < mediaDf.iloc[i,kpi_min_index] and mediaDf.iloc[i,2] < mediaDf.iloc[i,kpi_min_index]:
                ret1 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比{costOp}{mediaDfCopy.iloc[i,7]}，存在风险。\n'
            elif mediaDf.iloc[i,1] < mediaDf.iloc[i,kpi_min_index] and mediaDf.iloc[i,2] >= mediaDf.iloc[i,kpi_min_index]:
                ret1 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比{costOp}{mediaDfCopy.iloc[i,7]}，有所好转，但仍旧存在风险。\n'
            elif mediaDf.iloc[i,1] >= mediaDf.iloc[i,kpi_min_index] and mediaDf.iloc[i,2] < mediaDf.iloc[i,kpi_min_index]:
                ret2 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比{costOp}{mediaDfCopy.iloc[i,7]}，有所好转。\n'
            elif mediaDf.iloc[i,1] >= mediaDf.iloc[i,kpi_min_index] and mediaDf.iloc[i,2] >= mediaDf.iloc[i,kpi_min_index]:
                ret2 += f'{mediaDf.iloc[i,0]} 上周期ROI7D与KPI比较{mediaDfCopy.iloc[i,8]}，本周期ROI7D与KPI比较{mediaDfCopy.iloc[i,9]}，cost环比{costOp}{mediaDfCopy.iloc[i,7]}，表现稳定。\n'
            else:
                # 不做评价
                pass

        filename = getFilename(f'report2Text_{media}_1','txt')
        with open(filename,'w') as f:
            f.write(ret1)

        filename = getFilename(f'report2Text_{media}_2','txt')
        with open(filename,'w') as f:
            f.write(ret2)

if __name__ == '__main__':
    directory = '/src/data/report/海外iOS里程碑进度日报_20240115/'
    text2Fix(directory)