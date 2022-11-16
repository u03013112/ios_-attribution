# 用来对mod文件做整理，主要针对ModelCheckpoint保存了过多的mod
import os

class ModSaveFile:
    def __init__(self,path,suffix):
        self.path = path
        self.suffix = suffix
        self.reflush()

    def reflush(self):
        path = self.path
        suffix = self.suffix
        modList = []
        g = os.walk(path)
        for path,dirList,fileList in g:  
            for fileName in fileList:  
                if fileName.startswith(suffix):
                    
                    modFileName=os.path.join(path, fileName)
                    loss = fileName[:-3].split('-')[-1]
                    
                    modList.append({
                        'path':modFileName,
                        'loss':float(loss)
                    })
        modList = sorted(modList,key=lambda x:x['loss'])
        self.modList = modList
    
    def purge(self,saveBestNum=5):
        if len(self.modList) > 5:
            for i in range(5,len(self.modList)):
                modFileName = self.modList[i]['path']
                os.remove(modFileName)
        self.reflush()

    def getBestModFilePath(self):
        if len(self.modList) > 0:
            return self.modList[0]
        return "not found any mod"

if __name__ == '__main__':
    for i in [2,3,4,5,6,7,8,9,10,11,12,13,14,28]:
        m = ModSaveFile('/src/src/predSkan/total/mod/ema/','modV3_ema%d_'%i)
        m.purge()
        print(m.getBestModFilePath())

    # m = ModSaveFile('/src/src/predSkan/mod/geo/','modUS')
    # m.purge()
    # print(m.getBestModFilePath())