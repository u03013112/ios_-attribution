from newsapi import NewsApiClient

import json
import datetime

import sys
sys.path.append('/src')

from src.config import news_api_key

# Init
newsapi = NewsApiClient(api_key=news_api_key)

todayStr = datetime.datetime.now().strftime('%Y-%m-%d')
yesterdayStr = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

# /v2/everything
all_articles = newsapi.get_everything(
    q='ai',
    # sources='bbc-news,the-verge',
    # domains='bbc.co.uk,techcrunch.com',
    from_param=yesterdayStr,
    to=todayStr,
    language='en',
    sort_by='popularity',
    pageSize=10
    # page=2
)

# # /v2/top-headlines/sources
# sources = newsapi.get_sources()

# print(all_articles)

try:
    articles = json.dumps(all_articles)