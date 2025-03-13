# 🌤️ Weather Data Pipeline with Apache Airflow & BigQuery

## 📌 Overview

This project automates the collection, processing, and analysis of weather data using **Apache Airflow**, **BigQuery**, and **Python**.  
It fetches real-time weather data from OpenWeatherMap, processes it using Pandas, and stores it in Google BigQuery for further analysis.

## 🚀 Installation & Setup

### 1️⃣ Clone Repository

```sh
git clone https://github.com/your-repo/weather_pipeline.git
cd weather_pipeline
```

### 2️⃣ Configure Credentials

- Move your **Google Cloud Service Account JSON** file to `config/`:

  ```sh
  mv path/to/your-service-account.json config/service_account.json  # Linux/Mac
  move path\to\your-service-account.json config\service_account.json # Windows

  ```

### 3️⃣ Install Dependencies

```sh
pip install -r requirements.txt
```

### 4️⃣ Run Apache Airflow using Docker

```sh
docker-compose up -d --build
```

### 5️⃣ Trigger DAG in Airflow

#### Option 1: Run from Airflow UI

Go to [Airflow UI](http://localhost:8080), search for `weather_pipeline`, and enable it.

#### Option 2: Run from CLI and enable DAG

```sh
docker-compose exec airflow-webserver airflow dags trigger weather_pipeline
docker-compose exec airflow-webserver airflow dags unpause weather_pipeline

```

## 📊 Querying & Exporting Results

### Query 1: Weekly Average Temperature per City

```sql
SELECT
    city,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)) AS week_start
FROM `my-bigquery-project-453503.weather_dataset.weather_table`
WHERE DATE(timestamp) >= DATE_TRUNC(CURRENT_DATE(), WEEK)
GROUP BY city, week_start
ORDER BY city, week_start;
```

### Query 2: City with Highest Humidity (Last 7 Days)

```sql
SELECT city,
       MAX(humidity) AS max_humidity
FROM `my-bigquery-project-453503.weather_dataset.weather_table`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY city
ORDER BY max_humidity DESC
LIMIT 1;
```

### Query 3: Day with Highest Wind Speed in Hanoi

```sql
SELECT DATE(timestamp) AS date,
       MAX(wind_speed) AS max_wind_speed
FROM `my-bigquery-project-453503.weather_dataset.weather_table`
WHERE city = 'Hanoi'
GROUP BY date
ORDER BY max_wind_speed DESC
LIMIT 1;
```

## 🐜 License

MIT License

✅ DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)) sẽ lấy ngày đầu tiên của tuần chứa timestamp, bắt đầu từ Thứ Hai. 🚀
