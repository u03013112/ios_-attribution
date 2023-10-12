import requests
from bs4 import BeautifulSoup

def getTitle(url):
    response = requests.get(url, allow_redirects=True)
    if response.history:
        print("Request was redirected")
        for resp in response.history:
            print(resp.status_code, resp.url)
        print("Final destination:")
        print(response.status_code, response.url)

    soup = BeautifulSoup(response.text, 'html.parser')
    return soup.title.string

# 使用示例
print(getTitle("http://google.com"))
