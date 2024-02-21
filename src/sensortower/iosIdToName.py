import requests

def iOSIdToName(app_id):
    url = f"https://itunes.apple.com/lookup?id={app_id}&country=us"

    response = requests.get(url)
    app_data = response.json()

    if app_data["resultCount"] > 0:
        app_name = app_data["results"][0]["trackName"]
        return app_name
    else:
        return "App not found."


if __name__ == "__main__":
    print(idToName("284882215"))  # Facebook