# 画一些图
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import sys
sys.path.append('/src')

from src.tools import getFilename

def totalCvR7():
    print('画图，大盘数据，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[df.cv == cv]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7_%d.png'%(cv))
        print('save to /src/data/totalCvR7_%d.png'%(cv))
        plt.clf()

def totalCvR7Less1000():
    print('画图，cv count 小于1000，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[(df.cv == cv) & (df['count'] < 1000)]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7L1k_%d.png'%(cv))
        print('save to /src/data/totalCvR7L1k_%d.png'%(cv))

def totalCvR7Less50():
    print('画图，cv count 小于50，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    for cv in range(64):
        dfCv = df.loc[(df.cv == cv) & (df['count'] < 50)]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7L50_%d.png'%(cv))
        print('save to /src/data/totalCvR7L50_%d.png'%(cv))

# 过滤掉平均7日付费过高的数据，最高每人400usd
def totalCvR7F1():
    print('画图，大盘数据，每个cv一张图，count是x，7日总收入是y')
    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))
    df.loc[df.sumr7usd > df['count'] * 400,'sumr7usd']=df['count']*400
    for cv in range(64):
        dfCv = df.loc[df.cv == cv]
        count = dfCv['count'].to_numpy()
        r7 = dfCv['sumr7usd'].to_numpy()
        plt.title("cv = %d"%cv) 
        plt.xlabel("count") 
        plt.ylabel("r7usd sum") 
        plt.plot(count,r7,'ro')
        plt.savefig('/src/data/totalCvR7F1_%d.png'%(cv))
        print('save to /src/data/totalCvR7F1_%d.png'%(cv))
        plt.clf()

# 和geo相关的一些
def geoAbout():

    geoList = [
        {'name':'US','codeList':['US'],'usdLine':[],'color':''},
        {'name':'CA','codeList':['CA'],'usdLine':[],'color':''},
        {'name':'AU','codeList':['AU'],'usdLine':[],'color':''},
        {'name':'GB','codeList':['GB'],'usdLine':[],'color':''},
        {'name':'NZ','codeList':['NZ'],'usdLine':[],'color':''},
        {'name':'DE','codeList':['DE'],'usdLine':[],'color':''},
        {'name':'FR','codeList':['FR'],'usdLine':[],'color':''},
        {'name':'KR','codeList':['KR'],'usdLine':[],'color':''},
        {'name':'GCC','codeList':['AE','BH','KW','OM','QA','ZA','SA'],'usdLine':[],'color':''}
    ]

    geoDf = pd.read_csv(getFilename('totalGeoData4_20220501_20220930'))
    totalDf = pd.read_csv(getFilename('totalData20220501_20220930'))

    # 按日期，7日付费金额划线，累计线
    dateList = []
    totalUsdLine = []

    sinceTime = datetime.datetime.strptime('20220501','%Y%m%d')
    unitlTime = datetime.datetime.strptime('20220930','%Y%m%d')
    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        dateList.append(dayStr)
        totalUsd = totalDf.loc[totalDf.install_date == dayStr,'sumr7usd'].sum()
        totalUsdLine.append(totalUsd)

        for geo in geoList:
            usd = geoDf.loc[(geoDf.install_date == dayStr) & (geoDf.geo == geo['name']),'sumr7usd'].sum()
            geo['usdLine'].append(usd)

        
    plt.title("") 
    plt.xlabel("date 20220501->20220930") 
    plt.ylabel("r7usd sum") 
    plt.plot(dateList,totalUsdLine,'r-')
    print(sum(totalUsdLine))
    for geo in geoList:
        plt.plot(dateList,geo['usdLine'],'-')
        usdSum = sum(geo['usdLine'])
        print(geo['name'],usdSum)
    
    plt.savefig('/src/data/geo.png')
    print('save to /src/data/geo.png')
    plt.clf()



if __name__ == '__main__':
    # totalCvR7()
    # totalCvR7Less1000()
    # totalCvR7Less50()
    # totalCvR7F1()
    geoAbout()