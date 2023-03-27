import json
import requests 


# Notifier

def messageDiscord(title,description):

    url = "https://discord.com/api/webhooks/1057136017968201788/0PW1ARwjXhvg5UD7lyI8a-mPlbIBVju_S4xspUFIKKChrkxz-CPtfqDMl5o-oCOU5guJ"
  
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
