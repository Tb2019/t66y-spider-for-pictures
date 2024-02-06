from t66y_system.storage.redis_cli import RedisClient
from t66y_system.save.saver import Saver
from t66y_system.crawlers.index_crawler_copy import Clawer
import time

redis = RedisClient()
saver = Saver(count=1)
clawer = Clawer(start_page=1, totle=1, type=1)

if __name__ == '__main__':
    clawer.run()
    # time.sleep(5)
    while True:
        if redis.count() == 0:
            break
        saver.save_all()

