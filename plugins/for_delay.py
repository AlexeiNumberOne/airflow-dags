import redis
import logging
import time

from datetime import datetime

def check_pause():
    r = redis.Redis(
                host='redis',
                port=6379,
                db=0,
                decode_responses=True  
            )
    request = r.hgetall("wait")


    if not request:
        r.hset('wait', mapping={
            'start': datetime.now().isoformat(),
            'count': 1
        })
        r.expire('wait', 60)
        return

    start = datetime.fromisoformat(request['start'])
    count = int(request['count'])

    time_passed = (datetime.now() - start).total_seconds()
    
    if time_passed >= 60:
        r.hset('wait', mapping={
            'start': datetime.now().isoformat(),
            'count': 1
        })
        r.expire('wait', 60)
        return
    
    if count >= 5:
        wait = 65 - time_passed

        if wait > 0:
            logging.info(f"Достигнут лимит запросов, пауза {wait:.2f} секунд")
            time.sleep(wait)

        r.hset('wait', mapping={
            'start': datetime.now().isoformat(),
            'count': 1
        })
        r.expire('wait', 60)
        return

    count += 1
    r.hset('wait', mapping={
        'start': start.isoformat(),
        'count': count
    })
    r.expire('wait', 60)
