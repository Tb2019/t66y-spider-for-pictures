import asyncio
import aiohttp
import aiohttp_socks
from fake_headers import Headers
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ProcessPoolExecutor, wait, process
import time
from t66y_system.storage.redis_cli import RedisClient

base_url = 'https://t66y.com/thread0806.php?fid=8&search=&type={type}&page={page}'
proxy = 'socks5://localhost:10808'
semaphore = asyncio.Semaphore(20)

headers = Headers(headers=True).generate()

class Clawer(object):
    """
    get all the title and url needed
    """
    def __init__(self, start_page=1, totle=5, type=1):
        """
        instractions
        :param start_page: the first page you want to download
        :param totle: how many pages do you want to download
        :param type: 1-亚洲(default), 2-欧美, 3-动漫, 4-写真, 5-其他, None-混合主题
        """
        # self.loop = asyncio.get_event_loop()
        # self.loop.close()
        # self.session = None  # aiohttp.ClientSession(headers=headers, connector=conn, loop=self.loop)
        # self.pool = ProcessPoolExecutor(5)
        self.urls = [base_url.format(type=type, page=page) for page in range(start_page, start_page + totle)]

    async def crawl_index(self, url, session, title=None):
        async with semaphore:
            async with session.get(url) as resp:
                if not title:
                    return await resp.text()
                return title, await resp.text()

    def get_index_pages(self):
        loop = asyncio.get_event_loop()
        conn = aiohttp_socks.ProxyConnector.from_url(proxy)
        session = aiohttp.ClientSession(headers=headers, connector=conn, loop=loop)
        tasks = [self.crawl_index(url, session) for url in self.urls]
        index_pages = loop.run_until_complete(asyncio.gather(*tasks))
        conn.close()
        return index_pages

    def parse_index(self, html) -> dict:
        page = BeautifulSoup(html, 'html.parser')
        tbody = page.find('tbody', {'id': 'tbody'})
        h3_s = tbody.find_all('h3')
        result = {}
        for h3 in h3_s:
            title = h3.text
            a = h3.find('a')
            href = a.get('href')
            href = urljoin(base_url, href)
            result[title] = href
        return result

    def get_detail_pages(self, title_href: dict):
        loop = asyncio.get_event_loop()
        conn = aiohttp_socks.ProxyConnector.from_url(proxy)
        session = aiohttp.ClientSession(headers=headers, connector=conn, loop=loop)
        tasks = [self.crawl_index(href, session=session, title=title) for title, href in title_href.items()]
        detail_pages = loop.run_until_complete(asyncio.gather(*tasks))
        conn.close()
        return detail_pages

    def parse_detail(self, title_html: tuple):
        # urls = []
        title, html = title_html
        page = BeautifulSoup(html, 'html.parser')
        div = page.find('div', {'id': 'conttpc'})
        imgs = div.find_all('img')
        for img in imgs:
            url = img.get('ess-data')
            # urls.append(url)
            yield title, url
        # return urls

    def run(self):
        # process = ProcessPoolExecutor(1)
        # index_pages = process.submit(self.get_index_pages)
        # wait([index_pages])
        # process.shutdown()
        # 再次开启进程池处理每个页面
        # with ProcessPoolExecutor(5) as exe:
        #     processes = []
        #     for index_page in index_pages.result():
        #         p = exe.submit(self.parse_index, index_page)
        #         processes.append(p)
        #     wait(processes)
        # 合并每个页面中的标题和链接，集中爬取。可以省略这一步，在parse_index中直接高并发，并发量是 每页条目数 * 进程数
        #     title_href = {}
        #     for p in processes:
        #         title_href.update(p.result())
        #     print(title_href)
            # for title, href in title_href.items():
            #     exe.submit()
        index_pages = self.get_index_pages()
        with ProcessPoolExecutor(5) as exe:
            processes = []
            for index_page in index_pages:
                p = exe.submit(self.parse_index, index_page)
                processes.append(p)
            wait(processes)
            title_href = {}
            for p in processes:
                title_href.update(p.result())
        # 子页面获取和parse
            detail_pages = self.get_detail_pages(title_href)

        self.redis = RedisClient()
        for detail_page in detail_pages:
            gen = self.parse_detail(detail_page)
            for title, url in gen:
                self.redis.add(title, url)
                # yield title, url

    # def save(self):
    #     self.redis = RedisClient()
    #     for title, url in self.run():
    #         self.redis.add(title, url)


if __name__ == '__main__':
    cra = Clawer(totle=1)
    cra.run()