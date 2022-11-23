import pandas as pd


# 将反复添加Unnamed的csv清理一下
def purgeRetCsv(retCsvFilename):
    retDf = pd.read_csv(retCsvFilename)
    retDf.loc[:,~retDf.columns.str.match('Unnamed')].to_csv(retCsvFilename)

# 输入ret.csv的文件全路径，将新的结论写入到log.txt
# titleStr 是写在log.txt的第一行
def logUpdate(retCsvFilename,logTxtFilename,titleStr):
    retDf = pd.read_csv(retCsvFilename)
    with open(logTxtFilename, 'w') as f:
        # 只记录前5条
        df = retDf.loc[retDf.groupby('message').val_loss.idxmin()].reset_index(drop=True)
        df = df.sort_values(by=['val_loss'])
        # print(df)
        lines = '%s\n'%titleStr
        for i in range(len(df)):
            lines += 'mape:%.2f%%,path:%s,message:%s\n'%(df.iloc[i].at['val_loss'],df.iloc[i].at['path'],df.iloc[i].at['message'])
        f.write(lines)



if __name__ == '__main__':
    for geo in ['US','GCC','KR','T1']:
        purgeRetCsv('/src/data/doc/geo/%s/ret.csv'%geo)
        logUpdate('/src/data/doc/geo/%s/ret.csv'%geo,'/src/data/doc/geo/%s/log.txt'%geo,'%s'%geo)