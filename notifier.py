import json
import requests 


# Notifier

def messageDiscord(title,description):

    url = "https://discord.com/api/webhooks/1091136013423890535/TojgkAGWqRFxO121stGxjoAqBm0O-dIOohgjM3kCZbuF755z7S17ZieUDeTVTIYo4jYj"
  
    payload = json.dumps({
    "embeds": [
        {
        "title": title,
        "description": description
        }
    ]
    })
    headers = {
    'Content-Type': 'application/json',
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.status_code)

    returnBolean = True if response.status_code==204 else False
    
    return returnBolean
