import pandas as pd
# cvMap here
afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')

def getFilename(filename):
    return '/src/data/%s.csv'%(filename)

# 在原有df的基础上加一列来表示
def cvToUSD2(retDf):
    # 列名 usd
    # retDf.loc[:,'usd'] = 0
    retDf.insert(retDf.shape[1],'usd',0)
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        avg = (min_event_revenue + max_event_revenue)/2
        if pd.isna(max_event_revenue):
            avg = 0
        count = retDf.loc[retDf.cv==i,'count']
        retDf.loc[retDf.cv==i,'usd'] = count * avg
    return retDf

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()