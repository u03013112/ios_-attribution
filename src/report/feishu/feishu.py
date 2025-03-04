# 飞书的一些基础方法
import os
import json
import sys
sys.path.append('/src')

from src.config import fsAppId,fsAppSecret

import requests
from requests_toolbelt import MultipartEncoder

# curl --location --request POST 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal' \
# --header 'Content-Type: application/json' \
# --data-raw '{
#     "app_id": "",
#     "app_secret": ""
# }
# '

def getTenantAccessToken():
    url = f'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/'
    headers = {
        'Content-Type':'application/json'
    }
    data = {
        'app_id':fsAppId,
        'app_secret':fsAppSecret
    }
    r = requests.post(url,headers=headers,json=data)
    return r.json()['tenant_access_token']

# curl --location --request POST 'https://open.feishu.cn/open-apis/docx/v1/documents' \
# --header 'Authorization: Bearer t-' \
# --header 'Content-Type: application/json' \
# --data-raw '{
#   "folder_token": "RRlBfMuTglqODCdjmeYcoOVMn2f",
#   "title": "一篇新的文档2"
# }'
def createDoc(tenantAccessToken,title,folder_token = 'RRlBfMuTglqODCdjmeYcoOVMn2f'):
    url = f'https://open.feishu.cn/open-apis/docx/v1/documents'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }
    data = {
        'folder_token':folder_token,
        'title':title
    }
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 200:
        return response.json()['data']['document']['document_id']
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

# curl --location --request POST 'https://open.feishu.cn/open-apis/docx/v1/documents/JbVldgaIOoTj0SxAsXhcD9A6n2b/blocks/JbVldgaIOoTj0SxAsXhcD9A6n2b/children' \
# --header 'Authorization: Bearer t-' \
# --header 'Content-Type: application/json' \
# --data-raw '{
#     "children": [
#         {
#             "block_type": 3,
#             "heading1": {
#                 "elements": [
#                     {
#                         "text_run": {
#                             "content": "这是标题1"
#                         }
#                     }
#                 ]
#             }
#         }
#     ]
# }'
def addHead1(tenantAccessToken,documentId,blockId,title):
    # blockId 为空的时候 blockId = documentId

    if not blockId or blockId == '':
        blockId = documentId

    url = f'https://open.feishu.cn/open-apis/docx/v1/documents/{documentId}/blocks/{blockId}/children'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }
    data = {
        'children': [
            {
                'block_type': 3,
                'heading1': {
                    'elements': [
                        {
                            'text_run': {
                                'content': title
                            }
                        }
                    ]
                }
            }
        ]
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")


def addHead2(tenantAccessToken,documentId,blockId,title):
    # blockId 为空的时候 blockId = documentId

    if not blockId or blockId == '':
        blockId = documentId

    url = f'https://open.feishu.cn/open-apis/docx/v1/documents/{documentId}/blocks/{blockId}/children'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }
    data = {
        'children': [
            {
                'block_type': 4,
                'heading2': {
                    'elements': [
                        {
                            'text_run': {
                                'content': title
                            }
                        }
                    ]
                }
            }
        ]
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

# text_color 1：红色 ,2：橙色 ,3：黄色 ,4：绿色 ,5：蓝色 ,6：紫色 ,7：灰色
def addText(tenantAccessToken,documentId,blockId,text,text_color = 0,bold = False):
    # blockId 为空的时候 blockId = documentId

    if not blockId or blockId == '':
        blockId = documentId

    url = f'https://open.feishu.cn/open-apis/docx/v1/documents/{documentId}/blocks/{blockId}/children'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }
    data = {
        'children': [
            {
                'block_type': 2,
                'text': {
                    'elements': [
                        {
                            'text_run': {
                                'content': text
                            }
                        }
                    ]
                }
            }
        ]
    }

    data['children'][0]['text']['elements'][0]['text_run']['text_element_style'] = {}

    if text_color != 0:
        data['children'][0]['text']['elements'][0]['text_run']['text_element_style']['text_color'] = text_color

    if bold:
        data['children'][0]['text']['elements'][0]['text_run']['text_element_style']['bold'] = bold

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

# https://open.feishu.cn/document/server-docs/docs/docs/docx-v1/document-block/create
def addCode(tenantAccessToken, documentId, blockId, code, language='Python', wrap=True):
    # 如果 blockId 为空，则将其设置为 documentId
    if not blockId or blockId == '':
        blockId = documentId

    # 构造请求 URL
    url = f'https://open.feishu.cn/open-apis/docx/v1/documents/{documentId}/blocks/{blockId}/children'

    # 请求头
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }

    languageMap = {
        'PlainText':1,
        'ABAP':2,
        'Ada':3,
        'Apache':4,
        'Apex':5,
        'Assembly Language':6,
        'Bash':7,
        'CSharp':8,
        'C++':9,
        'C':10,
        'COBOL':11,
        'CSS':12,
        'CoffeeScript':13,
        'D':14,
        'Dart':15,
        'Delphi':16,
        'Django':17,
        'Dockerfile':18,
        'Erlang':19,
        'Fortran':20,
        'FoxPro':21,
        'Go':22,
        'Groovy':23,
        'HTML':24,
        'HTMLBars':25,
        'HTTP':26,
        'Haskell':27,
        'JSON':28,
        'Java':29,
        'JavaScript':30,
        'Julia':31,
        'Kotlin':32,
        'LateX':33,
        'Lisp':34,
        'Logo':35,
        'Lua':36,
        'MATLAB':37,
        'Makefile':38,
        'Markdown':39,
        'Nginx':40,
        'Objective-C':41,
        'OpenEdgeABL':42,
        'PHP':43,
        'Perl':44,
        'PostScript':45,
        'Power Shell':46,
        'Prolog':47,
        'ProtoBuf':48,
        'Python':49,
        'R':50,
        'RPG':51,
        'Ruby':52,
        'Rust':53,
        'SAS':54,
        'SCSS':55,
        'SQL':56,
        'Scala':57,
        'Scheme':58,
        'Scratch':59,
        'Shell':60,
        'Swift':61,
        'Thrift':62,
        'TypeScript':63,
        'VBScript':64,
        'Visual Basic':65,
        'XML':66,
        'YAML':67,
        'CMake':68,
        'Diff':69,
        'Gherkin':70,
        'GraphQL':71,
        'OpenGL Shading Language':72,
        'Properties':73,
        'Solidity':74,
        'TOML':75,
    }

    languageEnum = languageMap[language]

    # 请求体
    payload = {
        "children": [
            {
                "block_type": 14,  # 修正为代码块的数字枚举值
                "code": {
                    "language": languageEnum,  # 代码语言
                    "wrap": wrap,  # 是否自动换行
                    "elements": [
                        {
                            "text_run": {
                                "content": code  # 代码内容
                            }
                        }
                    ]
                }
            }
        ]
    }

    # 发送请求
    response = requests.post(url, headers=headers, json=payload)

    # 输出结果
    if response.status_code == 200:
        print("代码块添加成功")
    else:
        print("添加失败:", response.status_code, response.text)

def addFile(tenantAccessToken,documentId,blockId,file_path,view_type = 2):
    # blockId 为空的时候 blockId = documentId

    if not blockId or blockId == '':
        blockId = documentId

    # 先添加一个空的文件占位置，获得文件在文档中的token
# curl --location --request POST 'https://open.feishu.cn/open-apis/docx/v1/documents/JbVldgaIOoTj0SxAsXhcD9A6n2b/blocks/JbVldgaIOoTj0SxAsXhcD9A6n2b/children' \
# --header 'Authorization: Bearer t-' \
# --header 'Content-Type: application/json' \
# --data-raw '{
#     "children": [
#         {
#             "block_type": 23,
#             "file": {
#                 "token": "",
#                 "view_type":2
#             }
                
#         }
#     ]
# }'
    url = f'https://open.feishu.cn/open-apis/docx/v1/documents/{documentId}/blocks/{blockId}/children'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }
    data = {
        'children': [
            {
                'block_type': 23,
                'file': {
                    'token': '',
                    'view_type':view_type
                }
                    
            }
        ]
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 200:
        raise Exception(f"addFile Error: {response.status_code}, {response.text}")
    
    blockId = response.json()['data']['children'][0]['children'][0]
    # 在针对这个文件token进行上传
# curl --location --request POST 'https://open.feishu.cn/open-apis/drive/v1/medias/upload_all' \
# --header 'Authorization: Bearer t-' \
# --form 'file_name="report3_1_google.csv"' \
# --form 'parent_type="docx_file"' \
# --form 'parent_node="doxcn2K2zUbzdiMIuvGdZ1xB2Wc"' \
# --form 'file=@"/Users/u03013112/Documents/doc/report/20231118_20231125/report3_1_google.csv"' \
# --form 'size="1426"'

    url = f'https://open.feishu.cn/open-apis/drive/v1/medias/upload_all'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}'
    }

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    form = {
        'file_name': file_name,
        'parent_type': 'docx_file',
        'parent_node': blockId,
        'size': str(file_size),
        'file': (open(file_path, 'rb'))
    }
    
    multi_form = MultipartEncoder(form)
    headers['Content-Type'] = multi_form.content_type
    
    response = requests.post(url, headers=headers, data=multi_form)

    if response.status_code != 200:
        raise Exception(f"uploadFile Error: {response.status_code}, {response.text}")

    file_token = response.json()['data']['file_token']

    # 再将文件token修改到文档中
# curl --location --request PATCH 'https://open.feishu.cn/open-apis/docx/v1/documents/JbVldgaIOoTj0SxAsXhcD9A6n2b/blocks/doxcn2K2zUbzdiMIuvGdZ1xB2Wc' \
# --header 'Authorization: Bearer t-' \
# --header 'Content-Type: application/json' \
# --data-raw '{
#     "replace_file":{
#         "token":"FBlNb3ka0oWABaxMrl6cYGYBnpg"
#     }
# }'
    url = f'https://open.feishu.cn/open-apis/docx/v1/documents/{documentId}/blocks/{blockId}'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }
    data = {
        'replace_file':{
            'token':file_token
        }
    }

    response = requests.patch(url, headers=headers, json=data)

    return

def addImage(tenantAccessToken, documentId, blockId, image_path):
    if not blockId or blockId == '':
        blockId = documentId

    url = f'https://open.feishu.cn/open-apis/docx/v1/documents/{documentId}/blocks/{blockId}/children'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }
    data = {
        'children': [
            {
                'block_type': 27,
                'image': {}
            }
        ]
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 200:
        raise Exception(f"addFile Error: {response.status_code}, {response.text}")
    
    # print(response.json())
    blockId = response.json()['data']['children'][0]['block_id']
    
    # 上传图片并获取 image_key
    url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"

    headers = {
        "Authorization": f"Bearer {tenantAccessToken}"
    }

    file_name = os.path.basename(image_path)
    file_size = os.path.getsize(image_path)

    form = {
        'file_name': file_name,
        "parent_type": "docx_image",
        'parent_node': blockId,
        'size': str(file_size),
        'file': (open(image_path, 'rb'))
    }
    multi_form = MultipartEncoder(form)
    headers['Content-Type'] = multi_form.content_type
    
    response = requests.post(url, headers=headers, data=multi_form)

    if response.status_code != 200:
        raise Exception(f"uploadFile Error: {response.status_code}, {response.text}")

    # print(response.json())
    file_token = response.json()['data']['file_token']


    url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{documentId}/blocks/{blockId}"

    headers = {
        "Authorization": f"Bearer {tenantAccessToken}",
        "Content-Type": "application/json"
    }

    data = {
        "replace_image": {
            "token": file_token
        }
    }

    response = requests.patch(url, headers=headers, json=data)
    # print(response.json())

    if response.json()['code'] != 0:
        print("addImage 失败")
        print(response)
    else:
        print("addImage 成功")
    
    return



# 获得chatId暂时没有封装
# curl --location --request GET 'https://open.feishu.cn/open-apis/im/v1/chats' \
# --header 'Authorization: Bearer t-'
def getAllChatId(tenantAccessToken):
    url = f'https://open.feishu.cn/open-apis/im/v1/chats'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}'
    }
    response = requests.get(url, headers=headers)
    
    return response.text

# curl --location --request POST 'https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id' \
# --header 'Authorization: Bearer t-' \
# --header 'Content-Type: application/json' \
# --data-raw '{
#     "receive_id": "oc_3e59fcc0d068e649245cee2478d6a8b9",
#     "msg_type": "text",
#     "content": "{\"text\":\"今日iOS海外AI速度报告：https://rivergame.feishu.cn/docx/IMF7dtGpvoPhA6xzglScjM23nHH\"}"
# }'
def sendMessage(tenantAccessToken,message,chatId = 'oc_3e59fcc0d068e649245cee2478d6a8b9'):
    url = f'https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id'
    headers = {
        'Authorization': f'Bearer {tenantAccessToken}',
        'Content-Type': 'application/json'
    }
    content = json.dumps({"text": message})
    data = {
        'receive_id':chatId,
        'msg_type':'text',
        'content':content
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 200:
        print('data:',data)
        raise Exception(f"sendMessage Error: {response.status_code}, {response.text}")
    
    
    return response.json()
    
# 封装，为了外部引用，直接给debug群发消息
def sendMessageDebug(message):
    token = getTenantAccessToken()
    sendMessage(token,message,'oc_1e418dff75881d2b0d85a5f701262cb8')

# 发到debug2群
def sendMessageDebug2(message):
    token = getTenantAccessToken()
    sendMessage(token,message,'oc_80121e99102b659ba2f565e0dce5d4c2')


def sendMessageToWebhook(message,webhook):
    """
        参考文档：https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot?lang=zh-CN

        curl -X POST -H "Content-Type: application/json" \
        -d '{"msg_type":"text","content":{"text":"request example"}}' \
        https://open.feishu.cn/open-apis/bot/v2/hook/****
    """

    # https://open.feishu.cn/open-apis/bot/v2/hook/571e5617-d93c-4b96-81db-f288fbefba32

    url = webhook
    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        'msg_type':'text',
        'content':{
            'text':message
        }
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 200:
        raise Exception(f"sendMessageToWebhook Error: {response.status_code}, {response.text}")
    
    return


def sendMessageToWebhook2(title,text,aText,aUrl,webhook):
    """
        参考文档：https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot?lang=zh-CN

        curl -X POST -H "Content-Type: application/json" \
        -d '{"msg_type":"text","content":{"text":"request example"}}' \
        https://open.feishu.cn/open-apis/bot/v2/hook/****
    """

    # https://open.feishu.cn/open-apis/bot/v2/hook/571e5617-d93c-4b96-81db-f288fbefba32

    url = webhook
    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": [
                        [{
                            "tag": "text",
                            "text": text
                        }, {
                            "tag": "a",
                            "text": aText,
                            "href": aUrl
                        }]
                    ]
                }
            }
        }
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 200:
        raise Exception(f"sendMessageToWebhook Error: {response.status_code}, {response.text}")
    
    return

if __name__ == '__main__':
    # print(getTenantAccessToken())
    # print(createDoc(getTenantAccessToken(),'一篇新的文档'))

    token = getTenantAccessToken()
    # sendMessage(token,'今日iOS海外AI速度报告：https://rivergame.feishu.cn/docx/FGWld7bQboqDJux6axPcx5TOnIc')
    # print(getAllChatId(token))
    # sendMessage(token,'debug','oc_1e418dff75881d2b0d85a5f701262cb8')

    print(getAllChatId(token))

#     sendMessageDebug('''
# lastwar预测服务器收入3~36服 2025-03-03 报告已生成
# 暂时没有预测到低于10美元或20美元的情况预测期间月平均最低日收入可能达到69.38美元
# 详细报告https://rivergame.feishu.cn/docx/UvhEdswzMo4FcZxOoA0cXFj1nOd
#     ''')

    sendMessageToWebhook2('hello','world','click me','https://www.baidu.com','https://open.feishu.cn/open-apis/bot/v2/hook/571e5617-d93c-4b96-81db-f288fbefba32')

