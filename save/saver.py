import asyncio
import time
from t66y_system.storage.redis_cli import RedisClient
from concurrent.futures import ProcessPoolExecutor, wait
from multiprocessing import Queue, Manager
import aiohttp
import aiohttp_socks
import aiofiles
import os
from fake_headers import Headers
from loguru import logger

proxy = 'socks5://localhost:10808'
headers = Headers(headers=True).generate()
semaphore = asyncio.Semaphore(20)


class Saver(object):
    def __init__(self, count: int, save_path: str = 'D:/Software/pycharm/Project/Spider/download'):
        """
        description
        :param count: how many keys get from redis for one child_process
        :param save_path: the path you want to save imgs
        """
        self.redis = None
        self.save_path = save_path
        self.count = count
        (not os.path.exists(self.save_path)) and os.makedirs(self.save_path)


    async def crawl(self, key, member, session):
        async with semaphore:
            try:
                async with session.get(member) as resp:
                    async with aiofiles.open(self.save_path + f'/{key}/{member.split("/")[-1]}', 'wb') as f:
                        await f.write(await resp.read())

                self.redis.srem(key, member)
            except:
                logger.warning(f'{key} : {member} download fail,will try again later')

    def down_load(self, keys):
        loop = asyncio.get_event_loop()
        conn = aiohttp_socks.ProxyConnector.from_url(proxy)
        session = aiohttp.ClientSession(headers=headers, loop=loop, connector=conn)
        for key in keys:
            (not os.path.exists(self.save_path + f'/{key}')) and os.makedirs(self.save_path + f'/{key}')
            logger.info(f'downloading {key}')
            members = self.redis.get_member(key)
            tasks = [self.crawl(key, member, session) for member in members]
            loop.run_until_complete(asyncio.gather(*tasks))
        conn.close()

    def save(self, queue):
        cursor = queue.get()
        self.redis = RedisClient()
        cursor, keys = self.redis.batch(cursor=cursor, count=self.count)
        queue.put(cursor)  # use this to do if...
        queue.put(cursor)
        # 并发下载
        self.down_load(keys)

    # def other_save(self, queue):
    #     self.redis = RedisClient()
    #     cursor = queue.get()
    #     cursor, keys = self.redis.batch(cursor=cursor, count=self.count)
    #     queue.put(cursor)
    #     queue.put(cursor)
    #     print(keys)

    def save_all(self):
        queue = Manager().Queue(2)
        queue.put(0)
        with ProcessPoolExecutor(5) as exe:
            exe.submit(self.save, queue)
            time.sleep(2)  # 确保子进程拿到queue,否则直接走到后面的break，消耗掉queue，子进程卡住
            while True:
                time.sleep(1)
                if queue.qsize() > 1:
                    # 注意不可以使用queue.qsize() > 0，因为两次put有时间差。如果在两次时间差内，主进程循环到了此处，便会进入if，执行queue.get()，然后再执行exe.submit(self.save, queue)，此时传入的queue是一个空的队列，造成新进程get（）不到，程序卡住。
                    if queue.get() == 0:
                        break
                    exe.submit(self.save, queue)
        logger.info('one round over,next round will start in 5s')
        time.sleep(3)
        # return




if __name__ == '__main__':
    saver = Saver(count=1)
    saver.save_all()

