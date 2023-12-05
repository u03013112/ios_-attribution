# 将生成的报告，目前仅支持iOS海外速读AI版

import os
import sys
sys.path.append('/src')

from src.report.feishu.feishu import getTenantAccessToken,createDoc,addHead1,addHead2,addText,addFile,sendMessage

def main(dirFilePath):
    # 如果文件夹不存在，则退出
    if not os.path.exists(dirFilePath):
        print('文件夹不存在')
        return
    

    # 获取飞书的token
    tenantAccessToken = getTenantAccessToken()

    # 创建文档，并用文件夹的名字命名
    dirFileName = dirFilePath.split('/')[-1]
    # print(dirFileName)
    docId = createDoc(tenantAccessToken,dirFileName)

    addHead1(tenantAccessToken,docId,'','分国家')
    with open(os.path.join(dirFilePath,'report1_1_ai.txt'),'r') as f:
        text = f.read()
        addText(tenantAccessToken,docId,'',text)

    csvFilePath1 = os.path.join(dirFilePath,'report1_1.csv')
    addFile(tenantAccessToken,docId,'',csvFilePath1)

    addHead1(tenantAccessToken,docId,'','分媒体')
    with open(os.path.join(dirFilePath,'report2_1_ai.txt'),'r') as f:
        text = f.read()
        addText(tenantAccessToken,docId,'',text)

    csvFilePath2 = os.path.join(dirFilePath,'report2_1.csv')
    addFile(tenantAccessToken,docId,'',csvFilePath2)

    for media in ['bytedanceglobal','facebook','google']:
        addHead2(tenantAccessToken,docId,'',media)
        
        with open(os.path.join(dirFilePath,f'report3_1_{media}_ai.txt'),'r') as f:
            text = f.read()
            addText(tenantAccessToken,docId,'',text)
        
        csvFilePath3 = os.path.join(dirFilePath,f'report3_1_{media}.csv')
        addFile(tenantAccessToken,docId,'',csvFilePath3)

    # 发送消息
    message = f'今日iOS海外AI速读报告：https://rivergame.feishu.cn/docx/{docId}'
    sendMessage(tenantAccessToken,message)
    
    

if __name__ == '__main__':
    main('/src/data/report/iOSWeekly20231118_20231125')
