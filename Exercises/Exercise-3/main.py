import boto3
import gzip
import io

# 1. Tải file .gz từ S3
def get_gz_file_stream(bucket, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    gz_stream = gzip.GzipFile(fileobj=response['Body'])
    return gz_stream

# 2. Lấy URI từ dòng đầu tiên của wet.paths.gz
def get_first_uri(gz_stream):
    for line in gz_stream:
        return line.decode('utf-8').strip()  # chỉ lấy dòng đầu tiên

# 3. Stream tệp dữ liệu thực từ dòng URI
def stream_and_print_file_from_s3(uri_path):
    # uri dạng: crawl-data/CC-MAIN-2022-05/segments/.../wet/...
    bucket = "commoncrawl"
    key = uri_path

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)

    # Stream nội dung từng dòng mà không load toàn bộ file vào bộ nhớ
    for line in response['Body'].iter_lines():
        print(line.decode('utf-8', errors='replace'))

def main():
    gz_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    bucket = 'commoncrawl'

    print("Đang tải tệp .gz từ S3...")
    gz_stream = get_gz_file_stream(bucket, gz_key)

    print("Lấy URI từ dòng đầu tiên...")
    uri_path = get_first_uri(gz_stream)
    print(f"URI đầu tiên: {uri_path}")

    print("Tải xuống và in tệp thực tế từ S3:")
    stream_and_print_file_from_s3(uri_path)

if __name__ == "__main__":
    main()
