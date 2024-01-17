# 将生成的报告，目前仅支持iOS海外速读AI版

import os
import sys
sys.path.append('/src')

from src.report.feishu.feishu import getTenantAccessToken,createDoc,addHead1,addHead2,addText,addFile,sendMessage,sendMessageDebug

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

    addHead1(tenantAccessToken,docId,'','文档说明')
    addText(tenantAccessToken,docId,'',f'本文档每天发布，针对海外iOS里程碑相关数据，数据均是满7日数据。\n比BI里程碑快一天，所以花费比BI的数值略大。\n其他说明请参考下面《名词解释》章节。\n')
    
    addHead1(tenantAccessToken,docId,'','里程碑')
    with open(os.path.join(dirFilePath,'report1Text_1Fix.txt'),'r') as f:
        text = f.read()
        addText(tenantAccessToken,docId,'',text,bold=True)

    with open(os.path.join(dirFilePath,'report1Text_2Fix.txt'),'r') as f:
        text = f.read()
        addText(tenantAccessToken,docId,'',text)

    with open(os.path.join(dirFilePath,'report1Text_3Fix.txt'),'r') as f:
        text = f.read()
        addText(tenantAccessToken,docId,'',text,text_color=2)
    
    with open(os.path.join(dirFilePath,'reportOrganicText_1.txt'),'r') as f:
        text = f.read()
        addText(tenantAccessToken,docId,'',text,text_color=2,bold=True)
    
    with open(os.path.join(dirFilePath,'reportOrganicText_2.txt'),'r') as f:
        text = f.read()
        addText(tenantAccessToken,docId,'',text,text_color=4,bold=True)

    addHead2(tenantAccessToken,docId,'','参考数据')

    addText(tenantAccessToken,docId,'','里程碑完成情况表：',bold=True)
    addText(tenantAccessToken,docId,'','其中统计了从里程碑开始~上周期开始，从里程碑开始~上周期结束，从里程碑开始~本周起结束 3个累计里程碑完成情况。然后通过对这3个数据求差，对上周期和本周期内里程碑完成情况进行统计。')
    csvFilePath1 = os.path.join(dirFilePath,'report1Fix.csv')
    addFile(tenantAccessToken,docId,'',csvFilePath1)

    # reportOrganic1
    addText(tenantAccessToken,docId,'','自然量占比情况表：',bold=True)
    addText(tenantAccessToken,docId,'','自然量占比从一定程度的反应了媒体表现情况，在大盘ROI稳定情况下，自然量占比越高，媒体表现越差。')
    csvFilePath2 = os.path.join(dirFilePath,'reportOrganic2.csv')
    addFile(tenantAccessToken,docId,'',csvFilePath2)

    addHead1(tenantAccessToken,docId,'','分国家+分媒体详细数据')

    addHead2(tenantAccessToken,docId,'','各媒体回收占比（包括自然量）')
    addText(tenantAccessToken,docId,'','媒体7日收入采用融合归因数据，自然量采用大盘数据减融合归因数据。')
    # reportOrganic2
    csvFilePath3 = os.path.join(dirFilePath,'reportOrganic1.csv')
    addFile(tenantAccessToken,docId,'',csvFilePath3)

    addHead2(tenantAccessToken,docId,'','媒体评价补充说明')
    addText(tenantAccessToken,docId,'','媒体KPI折算：由于最终的KPI是含自然量ROI，所以按照最近自然量回收占比折算媒体KPI，即无自然量下媒体ROI标准。取最近两周的自然量回收占比中较低、较高的比例，折算媒体ROI的较低标准和较高标准。',text_color=0)
    addText(tenantAccessToken,docId,'','媒体评价：如果低于较低的媒体折算KPI，就评价为较差；如果高于较高的媒体折算KPI，就评价为较好。',text_color=0)
    addText(tenantAccessToken,docId,'','然后再结合花费环比，对最近媒体表现进行风险评估。',text_color=0)
    addText(tenantAccessToken,docId,'','下面ROI7均是满7日数据。',text_color=0)
    addText(tenantAccessToken,docId,'','下面ROI24小时 是最近数据，这部分数据没有7日完整回收。',text_color=0)
    addText(tenantAccessToken,docId,'','请注意周期的时间范围。',bold=True)

    for media in ['bytedanceglobal','facebook','google']:
        addHead2(tenantAccessToken,docId,'',f'{media} ROI7 与 花费')
        
        with open(os.path.join(dirFilePath,f'report2Text_{media}_1.txt'),'r') as f:
            text = f.read()
            addText(tenantAccessToken,docId,'',text,text_color=2)

        with open(os.path.join(dirFilePath,f'report2Text_{media}_2.txt'),'r') as f:
            text = f.read()
            addText(tenantAccessToken,docId,'',text,text_color=4)

        csvFilePath3 = os.path.join(dirFilePath,f'report2_{media}.csv')
        addFile(tenantAccessToken,docId,'',csvFilePath3)

        # 24小时
        addHead2(tenantAccessToken,docId,'',f'{media} ROI24小时')

        with open(os.path.join(dirFilePath,f'report3Text_{media}_1.txt'),'r') as f:
            text = f.read()
            addText(tenantAccessToken,docId,'',text,text_color=2)

        with open(os.path.join(dirFilePath,f'report3Text_{media}_2.txt'),'r') as f:
            text = f.read()
            addText(tenantAccessToken,docId,'',text,text_color=4)

        csvFilePath4 = os.path.join(dirFilePath,f'report3_{media}.csv')
        addFile(tenantAccessToken,docId,'',csvFilePath4)

    addHead1(tenantAccessToken,docId,'','名词解释')
    addText(tenantAccessToken,docId,'','本周期 & 上周期：使用可获得的最近的两周的满7日数据。并分为本周期与上周期两周进行环比。请注意周期中提到的具体日期。\n')
    addText(tenantAccessToken,docId,'','周期结束的满7ROI：从里程碑开始，到周期结束时间的累计满7日ROI。\n')
    addText(tenantAccessToken,docId,'','周期内的满7ROI：从周期开始，到周期结束包含的累计满7日ROI。注意与周期结束的满7ROI区别，是开始时间不一样，一般的他们的值是不一样的。\n')
    addText(tenantAccessToken,docId,'','达标花费：满7ROI大于等于KPI的累计花费。以国家为维度，如果满7ROI小于KPI，则认为不达标，达标花费清零。\n')

    print(f'今日iOS海外AI速读报告：https://rivergame.feishu.cn/docx/{docId}')

    # # 发送消息
    message = f'海外iOS里程碑进度日报：https://rivergame.feishu.cn/docx/{docId}'
    # sendMessage(tenantAccessToken,message)
    sendMessageDebug(message)
    
    

if __name__ == '__main__':
    main('/src/data/report/iOSWeekly20231118_20231125')
