import time
import requests
from requests.adapters import HTTPAdapter
from urllib import parse

import sys
sys.path.append('/src')

from src.config import fsAppId,fsAppSecret

class Feishu:
    def __init__(self):
        s = requests.Session()
        s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
        s.mount('https://',HTTPAdapter(max_retries=3))
        url = 'https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal'
        headers = {
            'Content-Type': 'application/json; charset=utf-8'
        }
        data = {
            "app_id": fsAppId,
            "app_secret": fsAppSecret
        }
        r = s.post(url=url, headers=headers, json=data)
        retJson = r.json()
        if retJson['code'] == 0:
            self.accessToken = retJson['app_access_token']
        else:
            print(r.text)
        return

    def createDoc(self,folderToken,title):
        # curl --location --request POST 'https://open.feishu.cn/open-apis/docx/v1/documents?folder_token=fldcnbCHL8OAtkcYHnPzZi1yupN' \
        # --header 'Authorization: Bearer u-xxx' # 调用前请替换为真实的访问令牌
        s = requests.Session()
        s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
        s.mount('https://',HTTPAdapter(max_retries=3))
        # 由于事件可能会比较长，暂时不设置timeout
        url = 'https://open.feishu.cn/open-apis/docx/v1/documents'
        url += '?folder_token=%s'%(folderToken)

        headers = {
            'Authorization': 'Bearer %s'%(self.accessToken),
            'Content-Type': 'application/json; charset=utf-8'
        }

        data = {
            "folder_token": folderToken,
            "title": title
        }    
            
        r = s.post(url=url, headers=headers, json=data)
        print(r.text)


if __name__ == '__main__':
    # fs = Feishu()
    # fs.createDoc('fldcnZfuiKN54xFgFyRls96ZaEc','testDoc')

    r = requests.get('https://open.feishu.cn/open-apis/authen/v1/index?app_id=cli_a3d8b77d70bf500c&redirect_uri=https%3a%2f%2fopen.feishu.cn%2fdocument%2fuQjL04CN%2fucDOz4yN4MjL3gzM&state=RANDOMSTATE')
    print(r.text)