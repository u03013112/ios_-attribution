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

mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'applovin','codeList':['applovin_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'unity','codeList':['unityads_int']},
    {'name':'apple','codeList':['Apple Search Ads']},
    {'name':'facebook','codeList':['Social_facebook','restricted']},
    {'name':'snapchat','codeList':['snapchat_int']},
]

if __name__ == '__main__':
    for media in mediaList:
        for n in [3,7,14,28]:
            # '/src/src/predSkan/media/mod/mod%d_%s{epoch:05d}-{val_loss:.2f}.h5'%(n,name)
            m = ModSaveFile('/src/src/predSkan/media/mod/','modS%d_%s'%(n,media['name']))
            m.purge()
            print(m.getBestModFilePath())

    