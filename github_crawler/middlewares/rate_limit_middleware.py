import random
import time

from scrapy import signals


class RateLimitMiddleware:
    def __init__(self, crawler):
        self.crawler = crawler
        # Giả sử giới hạn mặc định của GraphQL API là 5000 points/giờ cho authenticated user
        self.remaining = 5000  # Số điểm còn lại (GraphQL dùng "points" thay vì request count)
        self.reset_time = 0  # Thời điểm reset rate limit
        self.logger = crawler.spider.logger
        self.min_delay = 5  # Thời gian chờ tối thiểu giữa các request (giây)

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def process_request(self, request, spider):
        """Kiểm tra rate limit trước khi gửi request"""
        current_time = int(time.time())
        if self.remaining <= 0 and current_time < self.reset_time:
            wait_time = self.reset_time - current_time
            self.logger.info(f"Preemptive rate limit check: Pausing for {wait_time} seconds until reset at {self.reset_time}")
            time.sleep(wait_time)
        # Thêm delay nhỏ để tránh secondary rate limit
        time.sleep(self.min_delay)
        return None

    def process_response(self, request, response, spider):
        """Xử lý response để cập nhật rate limit"""
        # GraphQL API trả về rate limit trong headers
        self.remaining = int(response.headers.get('X-RateLimit-Remaining', self.remaining))
        self.reset_time = int(response.headers.get('X-RateLimit-Reset', self.reset_time))
        self.logger.warning(f"Request limit remaining: {self.remaining}.")
        # Xử lý lỗi 403 (Forbidden) - thường là rate limit hoặc secondary rate limit
        if response.status == 403:
            self.logger.warning(f"Received 403 Forbidden for {request.url}. Checking rate limit...")
            if self.remaining <= 0:
                current_time = int(time.time())
                wait_time = max(self.reset_time - current_time, 0)
                self.logger.info(f"Primary rate limit exceeded. Pausing for {wait_time} seconds until reset at {self.reset_time}")
                time.sleep(wait_time)
                return request  # Retry request sau khi chờ
            else:
                # Có thể là secondary rate limit hoặc lỗi khác
                self.logger.warning(f"403 not due to primary rate limit. Possible secondary rate limit or access issue.")
                wait_time = random.uniform(60, 120)  # Đợi ngẫu nhiên 1-2 phút
                self.logger.info(f"Waiting {wait_time:.2f} seconds to bypass potential secondary rate limit.")
                time.sleep(wait_time)
                return request  # Retry request

        # Kiểm tra rate limit sau mỗi response thành công
        if self.remaining <= 0:
            current_time = int(time.time())
            wait_time = max(self.reset_time - current_time, 0)
            if wait_time > 0:
                self.logger.info(f"Rate limit exceeded. Pausing for {wait_time} seconds until reset at {self.reset_time}")
                time.sleep(wait_time)
                return request  # Retry request sau khi chờ

        # Giảm delay nếu còn nhiều quota
        if self.remaining > 1000:  # Ví dụ: nếu còn hơn 1000 points, giảm delay
            self.min_delay = max(2, self.min_delay - 1)
        elif self.remaining < 500:  # Nếu sắp hết quota, tăng delay
            self.min_delay = min(10, self.min_delay + 1)

        return response

    def process_exception(self, request, exception, spider):
        """Xử lý exception (ví dụ: network error)"""
        self.logger.error(f"Exception occurred: {exception}. Retrying request after delay.")
        wait_time = random.uniform(30, 60)  # Đợi ngẫu nhiên 30-60 giây
        time.sleep(wait_time)
        return request  # Retry request

    def spider_opened(self, spider):
        self.logger.info(f"RateLimitMiddleware enabled with initial remaining: {self.remaining}")