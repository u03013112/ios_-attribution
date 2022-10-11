# 整体预测，包括null预测和自然量预测，主要是整体预测，然后生成总体报告
import sys
sys.path.append('/src')

from src.organic import main as organicMain
from src.organic import getAFCvUsdSum,getSkanCvUsd
from src.null import main as nullMain

def main(sinceTimeStr,unitlTimeStr):
    predictOrganicUsdSum = organicMain(sinceTimeStr,unitlTimeStr)
    predictNullUsdSum = nullMain(sinceTimeStr,unitlTimeStr)
    print('预测自然量付费总金额：',predictOrganicUsdSum)
    print('预测null付费总金额：',predictNullUsdSum)
    afUsdSum = getAFCvUsdSum(sinceTimeStr,unitlTimeStr)
    print('AF付费总金额：',afUsdSum)
    skanUsdSum = getSkanCvUsd(sinceTimeStr,unitlTimeStr)
    print('skan付费总金额：',skanUsdSum)

    print('总金额差（skan付费总金额 + 预测自然量付费总金额 + 预测null总付费金额） / AF付费总金额：',(skanUsdSum + predictOrganicUsdSum + predictNullUsdSum)/afUsdSum)

if __name__ == "__main__":
    main('20220601','20220630')