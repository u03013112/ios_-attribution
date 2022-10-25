# 日志分析
import sys
sys.path.append('/src')

from src.predSkan.totalAI0 import groupList

def bestModByValLoss():
    for i in range(len(groupList)):
        logFilename = '/src/src/predSkan/log/log%d.log'%(i)

        bestModName = '-'
        # lossMin = 100
        valLossMin = 100
        for line in open(logFilename,'r'):
            strList = line.split(' ')
            try:
                modName = strList[0]
                # loss = float(strList[1])
                val_loss = float(strList[2])

                if val_loss < valLossMin:
                    bestModName = modName
                    valLossMin = val_loss
            except:
                # print(i)
                continue
        # if valLossMin > 80:
        #     print(i,valLossMin)
        print(i,bestModName,valLossMin)

def bestModByloss():
    for i in range(len(groupList)):
        logFilename = '/src/src/predSkan/log/log%d.log'%(i)

        bestModName = '-'
        lossMin = 100
        valLossMin = 100
        for line in open(logFilename,'r'):
            strList = line.split(' ')
            try:
                modName = strList[0]
                loss = float(strList[1])
                # val_loss = float(strList[2])
                # print(loss,val_loss)
                # if val_loss < valLossMin:
                #     bestModName = modName
                #     valLossMin = val_loss
                if loss < lossMin:
                    bestModName = modName
                    lossMin = loss
            except:
                # print(i)
                continue
        # if valLossMin > 80:
        #     print(i,valLossMin)
        print(i,bestModName,lossMin)


if __name__ == '__main__':
    print('val loss:')
    bestModByValLoss()
    print('loss:')
    bestModByloss()
        


