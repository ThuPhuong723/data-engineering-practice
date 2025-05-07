## Bài tập nhập môn kĩ thuật dữ liệu 
## Thông tin nhóm 
1. Vũ Ngọc Thu Phương 23696981
2. Lê Nhật Việt 23712661
3. Nguyễn Hoàng Nam 23718591
4. Nguyễn Minh Trí 23710141

Lab 8
Tình huống 1: Thiết kế Pipeline tự động thu thập và trực quan hóa dữ liệu

Phương pháp tiếp cận: Áp dụng mô hình ETL (Extract – Transform – Load) để xây dựng luồng xử lý dữ liệu. Sử dụng Apache Airflow nhằm tự động hóa toàn bộ quá trình.

Các bước thực hiện:

Bước 1: Tiến hành thu thập dữ liệu đầu vào từ tệp CSV hoặc hệ quản trị cơ sở dữ liệu như MySQL.

Bước 2: Phát triển script Python đảm nhận vai trò xử lý và trích xuất dữ liệu cần thiết.

Bước 3: Nạp dữ liệu đã xử lý vào hệ thống cơ sở dữ liệu PostgreSQL, đồng thời khai thác Pandas để kiểm tra, định dạng.

Bước 4: Thực hiện trực quan hóa dữ liệu bằng Power BI để hỗ trợ phân tích.

Tổng quan dự án:
Dự án mô phỏng một chuỗi xử lý dữ liệu kiểu ETL hoàn chỉnh trong bối cảnh thương mại điện tử. Dữ liệu được thu thập từ các nguồn định sẵn, sau đó trải qua bước chuyển đổi, xử lý rồi nạp vào hệ quản trị cơ sở dữ liệu (PostgreSQL hoặc MySQL). Apache Airflow đảm nhận vai trò tự động hóa và theo dõi luồng xử lý. Ngoài ra, hệ thống hỗ trợ khả năng mở rộng bằng plugin, dễ dàng kết nối với nhiều hệ cơ sở dữ liệu khác nhau, đảm bảo tính linh hoạt cao

Kết quả khi chạy thử :ra được cái giao hình hình con  voi Psadmin ,nhưng chưa trực quan hóa được 

![image](https://github.com/user-attachments/assets/f74af19e-5abb-4a56-bf69-015795972708)

Tình huống 2: Xây dựng Pipeline thu thập dữ liệu và huấn luyện mô hình học máy

Mục tiêu: Thiết lập và vận hành các DAGs trong Apache Airflow, từ mức độ cơ bản đến nâng cao, để điều phối pipeline cho mô hình học máy.

Công cụ và nền tảng sử dụng: Triển khai các pipeline thông qua Apache Airflow với khả năng hẹn lịch chạy, đảm bảo quy trình huấn luyện mô hình được tự động hóa theo thời gian định trước



## Lab 9
### Mục tiêu:

- Python data processing.
- csv, flat-file, parquet, json, etc.
- SQL database table design.
- Python + Postgres, data ingestion and retrieval.
- PySpark
- Data cleansing / dirty data.

## Exercise 1 – Tải và xử lý tệp từ Internet
## 1. Mục tiêu:
Giúp sinh viên thực hành kỹ năng lập trình Python qua tình huống mô phỏng thực tế: tự động tải xuống và xử lý tệp dữ liệu từ Internet.

## 2. Yêu cầu cụ thể:
Tạo thư mục downloads nếu chưa tồn tại.

Lần lượt tải các tệp .zip từ danh sách URL.

Giải nén các tệp .csv có trong .zip.

Xoá file .zip sau khi giải nén thành công.

Quản lý ngoại lệ nếu quá trình tải hoặc giải nén bị lỗi.

3. Kết quả mong đợi:
Sau khi chương trình hoàn tất, chỉ còn lại các file .csv trong thư mục downloads. Mỗi bước xử lý sẽ được in ra màn hình để theo dõi tiến trình. Các lỗi được thông báo rõ ràng, không làm chương trình dừng đột ngột.
![image](https://github.com/user-attachments/assets/74bd383d-404b-4592-88ae-e36fc9424ce9)



## Exercise 2 – Web Scraping, Download và Phân tích dữ liệu
## 1. Mục tiêu:
Phát triển ứng dụng Python có khả năng trích xuất dữ liệu từ web, tải tệp và phân tích bằng thư viện Pandas.

## 2. Các bước thực hiện:
Chuẩn bị môi trường chạy bằng Docker.

Viết script Python để thu thập dữ liệu web và xây dựng đường dẫn tệp cần tải.

Tải và lưu các tệp từ web.

Đọc và xử lý dữ liệu với Pandas, thực hiện các thao tác phân tích cơ bản.

Kiểm tra và xác minh đầu ra.

## 3. Tổng kết:
Bài tập giúp củng cố kiến thức về scraping, xử lý file và làm việc với Pandas. Docker được sử dụng để đảm bảo môi trường chạy ổn định. Một số lỗi điển hình như phân quyền hoặc lỗi HTTP cũng được xử lý linh hoạt.

![image](https://github.com/user-attachments/assets/8d08910b-78bf-418f-af0b-f6167db76eb6)




#### Exercise 3 – Làm việc với AWS S3 và Boto3
## 1. Mục tiêu:
Thực hành thao tác với Amazon S3 bằng thư viện Boto3 trong Python, tập trung vào việc xử lý tệp nén .gz.

## 2. Quy trình thực hiện:
Cài đặt môi trường làm việc với AWS.

Kết nối và tải file wet.paths.gz từ bucket public của Common Crawl.

Đọc trực tiếp nội dung từ file mà không ghi ra đĩa.

Truy xuất đường dẫn WET đầu tiên và tải nội dung tệp .wet.gz tương ứng.

(Nâng cao) Stream nội dung để giảm tải bộ nhớ.

## 3. Kết luận:
Hệ thống thực hiện thành công toàn bộ quy trình từ tải và xử lý dữ liệu trực tiếp từ AWS S3. Phần Extra Credit giúp nâng cao hiệu năng bằng kỹ thuật stream thay vì load toàn bộ vào RAM.

## Exercise 4 – Chuyển đổi JSON thành CSV
## 1. Mục tiêu:
Viết chương trình Python trong môi trường Docker để chuyển đổi dữ liệu có cấu trúc lồng nhau từ định dạng .json sang .csv.

## 2. Các bước triển khai:
Viết script Python duyệt thư mục data/ và các thư mục con.

Đọc từng file .json, làm phẳng cấu trúc dữ liệu.

Ghi kết quả thành .csv tương ứng với từng file gốc.

Viết Dockerfile và docker-compose để chạy chương trình trong container.

## 3. Kết quả:
Hệ thống xử lý thành công tất cả file JSON trong thư mục chỉ định. Dữ liệu được chuẩn hóa và lưu dưới dạng CSV dễ phân tích, có thể tiếp tục sử dụng cho các bước ETL khác.

## Exercise 5 – Thiết kế cơ sở dữ liệu và nạp dữ liệu vào PostgreSQL
## 1. Mục tiêu:
Rèn luyện kỹ năng thiết kế mô hình quan hệ từ dữ liệu CSV và sử dụng Python để kết nối, thao tác với PostgreSQL.

## 2. Các bước triển khai:
Khám phá dữ liệu CSV đầu vào.

Thiết kế lược đồ dữ liệu và viết câu lệnh tạo bảng trong schema.sql.

Sử dụng psycopg2 trong Python để tạo bảng và nhập dữ liệu.

Thiết lập Docker để tự động hóa quá trình nạp dữ liệu.

## 3. Kết quả đạt được:
Chạy được code không lỗi nhưng không in ra màn hình gì 



## Exercise 6 – Phân tích dữ liệu lớn với PySpark
## 1. Mục tiêu:
Làm quen với xử lý dữ liệu quy mô lớn sử dụng Spark trong môi trường Docker.

## 2. Lộ trình thực hiện:
Thiết lập Docker để chạy Spark.

Đọc tệp .zip, giải nén và xử lý tệp .csv.

Thực hiện truy vấn, lọc, thống kê bằng PySpark.

Xuất kết quả phân tích thành các tệp .csv.

## 3. Kết quả:
Vẫn chưa làm được ex 6 




## Exercise 7 – Sử dụng hàm tích hợp trong PySpark
## 1. Mục tiêu:
Tận dụng các hàm có sẵn trong pyspark.sql.functions để xử lý và phân tích dữ liệu mà không cần dùng UDF hay hàm tùy chỉnh.

## 2. Nội dung thực hiện:
Thêm metadata như tên file và ngày tạo.

Trích xuất thương hiệu từ model thiết bị.

Xếp hạng dung lượng lưu trữ.

Tạo khóa chính định danh từng dòng dữ liệu.

(Tùy chọn) Viết kiểm thử bằng PyTest để kiểm tra kết quả.

## 3. Tổng kết:
Dữ liệu được xử lý hoàn toàn bằng các hàm gốc của PySpark, đảm bảo hiệu năng cao. Môi trường Docker được cấu hình ổn định, hỗ trợ cả chạy chính và kiểm thử.




v
