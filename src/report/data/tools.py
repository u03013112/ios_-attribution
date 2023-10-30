

def getFilename1(filenamePrefix,startDayStr,endDayStr,directory,suffix = ''):
    filename = f'{directory}/{filenamePrefix}{startDayStr}_{endDayStr}_{suffix}.csv'
    return filename