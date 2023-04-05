import json
import requests 


# Notifier

def messageDiscord(title,description):

    url = "https://discord.com/api/webhooks/1092887242025996418/0jVDPCk6Giopwa2xSHZCdVXfoW0r0HJk36B_XdbmKl4dYl7Ypf-ywVjrx4PpJbJNlU32"
  
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
