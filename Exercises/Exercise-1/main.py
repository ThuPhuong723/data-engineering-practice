import os
import requests
import zipfile
from pathlib import Path

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
]

DOWNLOAD_DIR = Path("downloads")

def create_download_dir():
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    print(f"Created download folder: {DOWNLOAD_DIR.resolve()}")

def get_filename_from_url(url):
    return url.split("/")[-1]

def download_file(url):
    filename = get_filename_from_url(url)
    zip_path = DOWNLOAD_DIR / filename

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {filename}")
        return zip_path
    except Exception as e:
        print(f"Failed to download {filename}: {e}")
        return None

def unzip_file(zip_path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        print(f"Extracted: {zip_path.name}")
        zip_path.unlink()
    except Exception as e:
        print(f"Failed to unzip {zip_path.name}: {e}")

def main():
    create_download_dir()
    for idx, url in enumerate(download_uris, start=1):
        print(f"[{idx}/{len(download_uris)}] Processing: {url}")
        zip_path = download_file(url)
        if zip_path:
            unzip_file(zip_path)

if __name__ == "__main__":
    main()
