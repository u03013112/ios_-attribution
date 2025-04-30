from newsapi import NewsApiClient

import rpyc
import json
import datetime

import sys
sys.path.append('/src')

from src.config import news_api_key
from src.report.feishu.feishu import getTenantAccessToken,createDoc,addHead1,addHead2,addText,addFile,sendMessage,addImage,addCode,sendMessageToWebhook,sendMessageToWebhook2


def toGpt(text):
    content = '''
你是一个AI新闻助手，你需要做下面几件事：
1、从我给你的文档中提取与AI有关新闻，根据原有的条目与排序进行整理，原信息中有编号
2、如果不是中文，请翻译成中文
3、内容输出格式使用：
（标题，需要翻译）
content主要内容摘要（需要翻译）
新闻链接（点击跳转）
（中间空一行）
    '''

    textFix = f'''
下面是我给你的信息：
{text}
'''

    message = [
        {"role":"user","content":content},
        {"role":"user","content":text}
    ]

    conn = rpyc.connect("192.168.40.62", 10002,config={"sync_request_timeout": 300})
    message_str = json.dumps(message)  # 将message转换为字符串
    x = conn.root.getAiResp(message_str)
    # print(x)
    return x


# Init
newsapi = NewsApiClient(api_key=news_api_key)

todayStr = datetime.datetime.now().strftime('%Y-%m-%d')
yesterdayStr = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')

# /v2/everything
all_articles = newsapi.get_everything(
    q='aigc',
    # sources='bbc-news,the-verge',
    # domains='bbc.co.uk,techcrunch.com',
    from_param=yesterdayStr,
    to=todayStr,
    # language='en',
    sort_by='popularity',
    # sort_by='publishedAt',
    page_size=10
    # page=2
)

# # /v2/top-headlines/sources
# sources = newsapi.get_sources()

# print(all_articles)


articles = all_articles['articles']
print(articles[0])

news = []

for article in articles:
    title = article['title']
    description = article['description']
    url = article['url']
    content = article['content']
    news.append({
        'title': title,
        'description': description,
        'url': url,
        'content': content
    })

print(news)

# 将news变为text，以便于传入gpt
newsText = ''
for i in range(len(news)):
    newsText += f'{i+1}.{news[i]["title"]}\n'
    newsText += f'   {news[i]["description"]}\n'
    newsText += f'   {news[i]["url"]}\n\n'
    newsText += f'   {news[i]["content"]}\n\n'

print('newsText:', newsText)
response = toGpt(newsText)
# print('response:',response)


sendMessageToWebhook(response, 'https://open.feishu.cn/open-apis/bot/v2/hook/acceb43c-5da3-47a2-987f-fc7228449a9c')