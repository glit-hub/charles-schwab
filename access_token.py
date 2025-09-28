import requests
import base64
import dotenv
import os
import webbrowser
from loguru import logger

dotenv.load_dotenv()

app_key = os.getenv("app_key")
app_secret = os.getenv("app_secret")
token_file = "refresh_token.txt"
#redirect_uri = "https://127.0.0.1"

def read_refresh_token():
    try:
        with open("refresh_token.txt", "r") as f:
            lines = f.readlines()
            if lines:  # Check if the file is not empty
                refresh_token = lines[-1].strip()  # .strip() removes trailing newline characters
                return refresh_token

    except Exception:
        print("Please fix refresh_token file!")
        return None
    

def access_tokens():
    refresh_token_value = read_refresh_token()

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token_value,
    }
    headers = {
        "Authorization": f'Basic {base64.b64encode(f"{app_key}:{app_secret}".encode()).decode()}',
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(
        url="https://api.schwabapi.com/v1/oauth/token",
        headers=headers,
        data=payload,
    )

    if response.status_code == 200:
        token_data = response.json()
        #print(token_data)
        access_token = token_data['access_token']
        with open("access_token.txt", "a") as f:
            f.write(f"\n{access_token}")
            f.close()
        return access_token
    
    

if __name__ == "__main__":
    access_tokens()
