# 整体预测，包括null预测和自然量预测，主要是整体预测，然后生成总体报告
import sys
sys.path.append('/src')

from src.organic import main as organicMain
from src.organic import getAFCvUsdSum,getSkanCvUsd
from src.null import main as nullMain

def main(sinceTimeStr,unitlTimeStr):
    retStr = sinceTimeStr + '~' + unitlTimeStr + '\n'
    predictOrganicUsdSum = organicMain(sinceTimeStr,unitlTimeStr)
    predictNullUsdSum = nullMain(sinceTimeStr,unitlTimeStr)
    retStr += '预测自然量付费总金额：'+str(predictOrganicUsdSum) +'\n'
    retStr += '预测null付费总金额：'+str(predictNullUsdSum)+'\n'
    print('预测自然量付费总金额：',predictOrganicUsdSum)
    print('预测null付费总金额：',predictNullUsdSum)
    afUsdSum = getAFCvUsdSum(sinceTimeStr,unitlTimeStr)
    print('AF付费总金额：',afUsdSum)
    retStr += 'AF付费总金额：'+str(afUsdSum)+'\n'
    skanUsdSum = getSkanCvUsd(sinceTimeStr,unitlTimeStr)
    print('skan付费总金额：',skanUsdSum)
    retStr += 'skan付费总金额：'+str(skanUsdSum)+'\n'

    print('总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：',(skanUsdSum + predictOrganicUsdSum + predictNullUsdSum)/afUsdSum)
    retStr += '总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：'+str((skanUsdSum + predictOrganicUsdSum + predictNullUsdSum)/afUsdSum) + '\n'
    return retStr

if __name__ == "__main__":
    retStr = ''
    # main('20220601','20220630')
    retStr += main('20220701','20220731')
    retStr += main('20220801','20220831')
    retStr += main('20220901','20220930')

    print(retStr)