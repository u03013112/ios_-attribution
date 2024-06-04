import requests

def iOSIdToName(app_id):
    url = f"https://itunes.apple.com/lookup?id={app_id}"

    response = requests.get(url)
    app_data = response.json()

    if app_data["resultCount"] > 0:
        app_name = app_data["results"][0]["trackName"]
        return app_name
    else:
        return iOSIdToNameCN(app_id)


def iOSIdToNameCN(app_id):
    url = f"https://itunes.apple.com/lookup?id={app_id}&country=cn"

    response = requests.get(url)
    app_data = response.json()

    if app_data["resultCount"] > 0:
        app_name = app_data["results"][0]["trackName"]
        return app_name
    else:
        return f"App not found.{app_id}"

def iOSIdToNameWithCountry(app_id,country):
    url = f"https://itunes.apple.com/lookup?id={app_id}&country={country}"

    response = requests.get(url)
    app_data = response.json()

    if app_data["resultCount"] > 0:
        app_name = app_data["results"][0]["trackName"]
        return app_name
    else:
        return f"App not found.{app_id}"

# 额外返回应用的 App Store 页面 URL
def iOSIdToNameWithCountry2(app_id,country):
    url = f"https://itunes.apple.com/lookup?id={app_id}&country={country}"

    response = requests.get(url)
    app_data = response.json()

    if app_data["resultCount"] > 0:
        app_name = app_data["results"][0]["trackName"]
        app_url = app_data["results"][0]["trackViewUrl"]
        return app_name,app_url
    else:
        return f"App not found.{app_id}",''

if __name__ == "__main__":
    # print(iOSIdToName("284882215"))  # Facebook
    print(iOSIdToName("1474572440")) 