from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import os

# Cấu hình mặc định cho DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),  # Bắt đầu từ ngày hôm qua
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Định nghĩa DAG với @dag decorator
@dag(
    schedule="0 5 * * *",  # Chạy vào 12:00 trưa giờ Việt Nam (UTC+7)
    default_args=default_args,
    catchup=False,  # TẮT chạy lại task cũ
    max_active_runs=1,  # ĐẢM BẢO chỉ có 1 task chạy cùng lúc
    tags=["weather", "pipeline"],
)
def weather_pipeline():

    # Hàm chạy subprocess với lỗi được ghi log
    def run_script(script_name):
        script_path = os.path.join(
            os.getenv("AIRFLOW_HOME", "/opt/airflow"), f"scripts/{script_name}"
        )
        try:
            subprocess.run(["python", script_path], check=True)
        except subprocess.CalledProcessError as e:
            print(f"❌ Lỗi khi chạy {script_name}: {e}")
            raise

    @task()
    def fetch_weather():
        run_script("fetch_weather.py")

    @task()
    def process_weather():
        run_script("process_weather.py")

    @task()
    def upload_to_bigquery():
        run_script("upload_to_bigquery.py")

    # Xây dựng pipeline tuần tự
    fetch_weather() >> process_weather() >> upload_to_bigquery()


# Khởi tạo DAG
dag = weather_pipeline()
