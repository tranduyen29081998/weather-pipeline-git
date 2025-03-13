import pandas as pd
import json
import os
from datetime import datetime

data = []
CITIES = ["Hanoi", "Ho Chi Minh City", "Da Nang"]

for city in CITIES:
    try:
        with open(f"data/{city}_weather.json", "r") as f:
            weather = json.load(f)
            record = {
                "city": city,
                "temperature": weather.get("main", {}).get("temp", None),
                "humidity": weather.get("main", {}).get("humidity", None),
                "wind_speed": weather.get("wind", {}).get("speed", None),
                "timestamp": datetime.fromtimestamp(
                    weather.get("dt", 0)
                ),  # Chuyển thành TIMESTAMP
            }
            data.append(record)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"❌ Lỗi khi xử lý {city}: {e}")

# Chuyển đổi dữ liệu vào DataFrame
df = pd.DataFrame(data)

# Đảm bảo cột timestamp có định dạng TIMESTAMP của BigQuery
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

# Lưu DataFrame vào CSV
os.makedirs("data", exist_ok=True)
df.to_csv("data/weather_data.csv", index=False)

print("✅ Dữ liệu đã lưu thành công vào weather_data.csv")
