from newsapi import NewsApiClient
import rpyc
import json
import datetime

import sys
sys.path.append('/src')
from src.config import news_api_key
from src.report.feishu.feishu import sendMessageToWebhook

class News:
    def __init__(self, q):
        self.q = q
        self.newsapi = NewsApiClient(api_key=news_api_key)
        self.todayStr = datetime.datetime.now().strftime('%Y-%m-%d')
        self.yesterdayStr = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')

    def fetch_news(self):
        all_articles = self.newsapi.get_everything(
            q=self.q,
            from_param=self.yesterdayStr,
            to=self.todayStr,
            sort_by='popularity',
            page_size=10
        )
        articles = all_articles['articles']
        news = []
        for article in articles:
            title = article['title']
            description = article['description']
            url = article['url']
            news.append({
                'title': title,
                'description': description,
                'url': url
            })
        return news

    def format_news(self, news):
        newsTextList = []
        for i, article in enumerate(news):
            newsText = f'{i+1}.{article["title"]}\n'
            newsText += f'   {article["description"]}\n'
            newsText += f'   {article["url"]}\n'
            newsTextList.append(newsText)
        return newsTextList

    def to_gpt(self, newsTextList):
        content = '''
你是一个AI新闻助手，你需要做下面几件事：
1、从我给你的文档中提取新闻，根据原有的条目与排序进行整理，原信息中有编号
2、如果不是中文，请翻译成中文
3、内容输出格式使用：
（标题，需要翻译）
新闻链接（点击跳转）
（中间空一行）

下面是我给你的信息：
        '''
        message = [{"role": "user", "content": content}]
        for newsText in newsTextList:
            message.append({"role": "user", "content": newsText})

        conn = rpyc.connect("192.168.40.62", 10002, config={"sync_request_timeout": 300})
        message_str = json.dumps(message)
        response = conn.root.getAiRespInSec(message_str)
        return response

    def send_news(self, response, webhook_url):
        sendMessageToWebhook(response, webhook_url)

if __name__ == "__main__":
    query = "Side Hustles"
    news_instance = News(query)
    news_data = news_instance.fetch_news()
    formatted_news = news_instance.format_news(news_data)
    response = news_instance.to_gpt(formatted_news)
    news_instance.send_news(response, 'https://open.feishu.cn/open-apis/bot/v2/hook/acceb43c-5da3-47a2-987f-fc7228449a9c')
    print("News related to 在家赚钱 has been sent.")
