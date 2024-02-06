import asyncio
import aiohttp
import aiohttp_socks
from fake_headers import Headers
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ProcessPoolExecutor


base_url = 'https://t66y.com/thread0806.php?fid=8&search=&type={type}&page={page}'
proxy = 'socks5://localhost:10808'
conn = aiohttp_socks.ProxyConnector.from_url(proxy)

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
        self.loop = asyncio.get_event_loop()
        self.loop.close()
        self.session = None  # aiohttp.ClientSession(headers=headers, connector=conn, loop=self.loop)
        self.urls = [base_url.format(type=type, page=page) for page in range(start_page, start_page + totle)]

    async def crawl_index(self, url):
        self.session = aiohttp.ClientSession(headers=headers, connector=conn, loop=self.loop)
        async with self.session.get(url) as resp:
            return await resp.text()

    def parse_index(self, html) -> dict:
        print(5555)
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
        print(result)
        return result

    def fn(self):
        print('hello')

    def run(self):
        # self.loop = asyncio.get_event_loop()
        # tasks = [self.crawl_index(url) for url in self.urls]
        # index_pages = self.loop.run_until_complete(asyncio.gather(*tasks))
        # print(index_pages)
        # conn.close()
        # self.loop.close()
        # self.session.close()
        with ProcessPoolExecutor(5) as exe:
            # for page in index_pages:
            for page in range(5):
                exe.submit(self.fn)







if __name__ == '__main__':
    cra = Clawer(totle=1)
    cra.run()