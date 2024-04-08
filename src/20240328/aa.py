# 阿里的aigc api的各种尝试
# Animate-anyone

import sys
sys.path.append('/src')

from src.config import apiKey

import requests



def image2video(image_url,model='animate-anyone-mivo',pose_sequence_id='m_02_jilejingtu_9s'):
    '''
        curl --location 'https://dashscope.aliyuncs.com/api/v1/services/aigc/image2video/video-synthesis/' \
        --header 'X-DashScope-Async: enable' \
        --header 'Authorization: Bearer apikey' \
        --header 'Content-Type: application/json' \
        --data '{
            "model": "animate-anyone-mivo",
            "input": {
                "image_url": "image_url",
                "pose_sequence_id": "luoli15"
            },
            "parameters": {
            }
        }'
    
    '''
    url = 'https://dashscope.aliyuncs.com/api/v1/services/aigc/image2video/video-synthesis/'
    headers = {
        'X-DashScope-Async': 'enable',
        'Authorization': f'Bearer {apiKey}',
        'Content-Type': 'application/json'
    }
    data = {
        "model": model,
        "input": {
            "image_url": image_url,
            "pose_sequence_id": pose_sequence_id
        },
        "parameters": {
        }
    }
    response = requests.post(url, headers=headers, json=data)

    # {"request_id":"0f6a4934-5262-99d4-bcfe-5d6a66b7ed5a","output":{"task_id":"1e8d2c14-5016-4beb-b0f9-fb0fd04fcfe5","task_status":"PENDING","submit_time":"2024-03-28 14:30:53.993","task_metrics":{"TOTAL":1,"SUCCEEDED":0,"FAILED":0}}}

    return response.json()

def getTaskStatus(task_id):
    '''
        curl -X GET \
        --header 'Authorization: Bearer api-key' \
        https://dashscope.aliyuncs.com/api/v1/tasks/{task_id}
    '''
    url = f'https://dashscope.aliyuncs.com/api/v1/tasks/{task_id}'
    headers = {
        'Authorization': f'Bearer {apiKey}',
    }
    response = requests.get(url, headers=headers)

    # {"request_id":"93e2a99f-ebe3-9bd1-a02c-498b09b8dda4","output":{"task_id":"1e8d2c14-5016-4beb-b0f9-fb0fd04fcfe5","task_status":"PENDING","submit_time":"2024-03-28 14:30:53.993","task_metrics":{"TOTAL":1,"SUCCEEDED":0,"FAILED":0}}}
    return response.json()


if __name__ == '__main__':
    # image_url = 'https://s21.ax1x.com/2024/03/28/pFoCcBn.png'
    # image_url = 'https://www.rivergame.net/zh/res/img/comm/home/heros/hero/airforce/7.jpg'
    # result = image2video(image_url)
    # print(result)

    # taskId = '81f69cc1-a601-4800-b99f-299a4cd175b5'
    # result = getTaskStatus(taskId)
    # print(result)
    