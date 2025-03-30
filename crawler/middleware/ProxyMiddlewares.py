import json
import random
import requests


class FreeProxyListMiddleware:
    PROXY_API_URL = "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc"
    PROXIES = ["socks4://169.255.136.8:60279"]
    TEST_URL = "http://httpbin.org/ip"  # URL để kiểm tra proxy

    def __init__(self):
        var = self.PROXIES
        # self.update_proxies()

    def update_proxies(self):
        try:
            response = requests.get(self.PROXY_API_URL)
            if response.status_code == 200:
                proxy_data = json.loads(response.text)
                all_proxies = [
                    f"{proxy['protocols'][0]}://{proxy['ip']}:{proxy['port']}"
                    for proxy in proxy_data.get("data", [])
                    if proxy.get("protocols")
                ]
                # Kiểm tra proxy hoạt động
                self.PROXIES = self.filter_working_proxies(all_proxies)
                if not self.PROXIES:
                    raise ValueError("No working proxies found")
                print(f"Loaded {len(self.PROXIES)} working proxies")
            else:
                raise Exception(f"Failed to fetch proxies: {response.status_code}")
        except Exception as e:
            print(f"Error updating proxies: {e}")

    def filter_working_proxies(self, proxies):
        working_proxies = []
        for proxy in proxies:
            try:
                # Kiểm tra proxy với timeout 5 giây
                r = requests.get(self.TEST_URL, proxies={"http": proxy, "https": proxy}, timeout=5)
                if r.status_code == 200:
                    working_proxies.append(proxy)
                    print(f"Proxy {proxy} is working")
            except Exception:
                continue
        return working_proxies

    def process_request(self, request, spider):
        if not self.PROXIES:
            self.update_proxies()

        if self.PROXIES:
            proxy = random.choice(self.PROXIES)
            request.meta["proxy"] = proxy
            spider.logger.info(f"Using proxy: {proxy}")
        else:
            spider.logger.warning("No working proxies available")
        return None

    def process_exception(self, request, exception, spider):
        spider.logger.error(f"Proxy failed: {exception}")
        if "proxy" in request.meta:
            failed_proxy = request.meta["proxy"]
            if failed_proxy in self.PROXIES:
                self.PROXIES.remove(failed_proxy)
                spider.logger.info(f"Removed failed proxy: {failed_proxy}")
        if self.PROXIES:
            new_proxy = random.choice(self.PROXIES)
            request.meta["proxy"] = new_proxy
            spider.logger.info(f"Retrying with new proxy: {new_proxy}")
            return request
        self.update_proxies()
        return None