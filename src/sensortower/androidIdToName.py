import requests
from bs4 import BeautifulSoup

def androidIdToName(app_id):
    url = f"https://play.google.com/store/apps/details?id={app_id}"

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        app_name = soup.find_all("h1")[0].text
        return app_name
    else:
        return "App not found."

def androidIdToName2(app_id):
    url = f"https://play.google.com/store/apps/details?id={app_id}"
    try:
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            app_name = soup.find_all("h1")[0].text
            return app_name, url
        else:
            return f"App not found.{app_id}", ''
    except:
        return f"App not found.{app_id}", ''
 

if __name__ == "__main__":
    print(androidIdToName("com.facebook.katana"))  # Facebook
    