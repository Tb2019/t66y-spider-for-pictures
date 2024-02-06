import redis
from loguru import logger


# REDIS_CLIENT_VERSION = redis.__version__
# print(REDIS_CLIENT_VERSION)

class RedisClient(object):
    """
    redis connection for title_urls
    """
    def __init__(self, host='localhost', port=6379, password='123456', db=1):
        self.db = redis.StrictRedis(host=host, port=port, password=password, db=db, decode_responses=True)

    # def exists(self, title):
    #     return self.db.exists(title)

    def add(self, title, url):
        # 不需要判断是否存在
        # self.db.zadd('title', {'w': 100})
        # if self.exists(title):
        #     logger.info(f'{title} is exist,please not save again')
            # return
        self.db.sadd(title.replace(':', '-'), url)

    def count(self):
        return self.db.dbsize()

    def batch(self, cursor, count):
        cursor, keys = self.db.scan(cursor=cursor, count=count)
        return cursor, keys

    def get_member(self, key):
        return self.db.smembers(key)

    def srem(self, key, value):
        self.db.srem(key, value)


if __name__ == '__main__':
    re = RedisClient()
    re.add('test', 'www.cn')
    # for i in range(50):
    #     re.add(f't{i}', 'he')
    # print(re.count())
    # re.add('t1', 'nihao')
    # re.batch(0, 10)


    # cursor = 0
    # while True:
    #     cursor, keys = re.batch(cursor, 5)
    #     print(len(keys))
    #     for key in keys:
    #         res = re.get_member(key)
    #         print(key, res)
    #     if not cursor:
    #         break
    #     break


