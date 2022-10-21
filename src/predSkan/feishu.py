import requests
import sys
import json
sys.path.append('/src')

from src.config import app_id,app_secret

def getToken():
    url= "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"   
    post_data = {
        "app_id": app_id,
        "app_secret": app_secret
    }
            
    r = requests.post(url, data=post_data)
    tat = r.json()["tenant_access_token"] 
    # print(tat)
    return tat

def addWorksheet(spreadsheetToken,tat):
    # https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/shtcnRQbjSmaWXZb3C4YmNWUxkh/sheets_batch_update
    url = 'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/sheets_batch_update'%(spreadsheetToken)
    header = {"Content-Type": "application/json", "Authorization": "Bearer " + str(tat)} #请求头
    post_data = {"requests": [
        {
            "addSheet": {
                "properties": {
                    "title": "0",
                    "index": 0
                }
            }
        }
    ]}
    r2 = requests.post(url, data=json.dumps(post_data), headers=header)  #请求写入
    print(r2.text)
    # print( r2.json()["msg"])  #输出来判断写入是否成功



if __name__ == '__main__':
    token = getToken()
    addWorksheet('shtcnRQbjSmaWXZb3C4YmNWUxkh',token)

