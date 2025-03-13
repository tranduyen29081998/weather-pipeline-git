import requests
import json
import os

CITIES = ["Hanoi", "Ho Chi Minh City", "Da Nang"]
API_KEY = os.getenv("WEATHER_API_KEY")
BASE_URL = os.getenv("WEATHER_BASE_URL")


def fetch_weather_data(city):
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        data = response.json()
        os.makedirs("data", exist_ok=True)
        with open(f"data/{city}_weather.json", "w") as f:
            json.dump(data, f)
        print(f"✅ Lưu thành công dữ liệu {city}")
    else:
        print(f"❌ Lỗi khi gọi API {city}: {response.status_code}, {response.text}")


for city in CITIES:
    fetch_weather_data(city)
