import pandas as pd

df = pd.read_csv('/src/data/customLayer/a_step3_SumDf.csv')
mediaList = df['media_group'].unique().tolist()
for media in mediaList:
    mediaDf = df[df['media_group'] == media]
    print(media)
    print(mediaDf.corr())

    print('%.2f%%'%(mediaDf['sumr7usd'].sum()/df['sumr7usd'].sum()*100))