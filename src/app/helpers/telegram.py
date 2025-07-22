import requests

TELEGRAM_BOT_TOKEN = "7803405882:AAH2GOyOBV86qeolMOEf3HK88XYITXn44cA"
TELEGRAM_CHAT_ID = "7453304006"

class Telegram:
    
    @staticmethod
    def send_alert(message: str):
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        try:
            response = requests.post(url, json=payload, timeout=5)
            response.raise_for_status()
        except Exception as e:
            print(f"[Telegram Alert Error] {e}")
