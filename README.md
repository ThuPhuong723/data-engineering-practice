## Bài tập nhập môn kĩ thuật dữ liệu 
## Thông tin nhóm 
1. Vũ Ngọc Thu Phương 23696981
2. Lê Nhật Việt 23712661
Lab 8
Tình huống 1: Thiết kế Pipeline tự động thu thập và trực quan hóa dữ liệu
Phương pháp tiếp cận: Áp dụng mô hình ETL (Extract – Transform – Load) để xây dựng luồng xử lý dữ liệu. Sử dụng Apache Airflow nhằm tự động hóa toàn bộ quá trình.

Các bước thực hiện:

Bước 1: Tiến hành thu thập dữ liệu đầu vào từ tệp CSV hoặc hệ quản trị cơ sở dữ liệu như MySQL.

Bước 2: Phát triển script Python đảm nhận vai trò xử lý và trích xuất dữ liệu cần thiết.

Bước 3: Nạp dữ liệu đã xử lý vào hệ thống cơ sở dữ liệu PostgreSQL, đồng thời khai thác Pandas để kiểm tra, định dạng.

Bước 4: Thực hiện trực quan hóa dữ liệu bằng Power BI để hỗ trợ phân tích.

Tổng quan dự án:
Dự án mô phỏng một chuỗi xử lý dữ liệu kiểu ETL hoàn chỉnh trong bối cảnh thương mại điện tử. Dữ liệu được thu thập từ các nguồn định sẵn, sau đó trải qua bước chuyển đổi, xử lý rồi nạp vào hệ quản trị cơ sở dữ liệu (PostgreSQL hoặc MySQL). Apache Airflow đảm nhận vai trò tự động hóa và theo dõi luồng xử lý. Ngoài ra, hệ thống hỗ trợ khả năng mở rộng bằng plugin, dễ dàng kết nối với nhiều hệ cơ sở dữ liệu khác nhau, đảm bảo tính linh hoạt cao.

Tình huống 2: Xây dựng Pipeline thu thập dữ liệu và huấn luyện mô hình học máy
Mục tiêu: Thiết lập và vận hành các DAGs trong Apache Airflow, từ mức độ cơ bản đến nâng cao, để điều phối pipeline cho mô hình học máy.

Công cụ và nền tảng sử dụng: Triển khai các pipeline thông qua Apache Airflow với khả năng hẹn lịch chạy, đảm bảo quy trình huấn luyện mô hình được tự động hóa theo thời gian định trước
