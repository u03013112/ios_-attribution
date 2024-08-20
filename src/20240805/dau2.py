# 读取st的媒体dau数据，判断是否存在周末效应，即周末的dau是否明显高于工作日
import pandas as pd
import chardet

def dauWeekday():
    df = pd.read_csv('双平台 DAU (Jan 1, 2024 - Aug 13, 2024, 所有国家-地区), 详细.csv')
    df = df[['Unified Name', 'Date', 'DAU']]
    df = df.groupby(['Unified Name', 'Date']).agg(
        {
            'DAU':'sum'
        }
    ).reset_index()
     # 转换日期列为日期类型
    df['Date'] = pd.to_datetime(df['Date'])
    
    # 提取星期信息，0表示星期一，6表示星期日
    df['Weekday'] = df['Date'].dt.weekday
    
    # 定义工作日和周末
    df['DayType'] = df['Weekday'].apply(lambda x: 'Weekend' if x >= 5 else 'Weekday')
    
    # 按照Unified Name和DayType分组，计算DAU的均值
    result = df.groupby(['Unified Name', 'DayType']).agg(
        {
            'DAU': 'mean'
        }
    ).reset_index()
    
    # 将结果转换为宽表格形式
    pivot_table = result.pivot(index='Unified Name', columns='DayType', values='DAU').reset_index()
    
    # 计算weekend/weekday比值
    pivot_table['weekend/weekday'] = pivot_table['Weekend'] / pivot_table['Weekday']
    
    # 重命名列
    pivot_table.columns.name = None
    pivot_table.rename(columns={'Unified Name': 'app id', 'Weekday': 'weekday', 'Weekend': 'weekend'}, inplace=True)
    
    # 打印结果
    print(pivot_table)

if __name__ == '__main__':
    dauWeekday()