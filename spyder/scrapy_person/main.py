# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import redis

redis_conn = redis.Redis(host='localhost', port=6379, db=0)
wobu = redis_conn.smembers('2')
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
for i in wobu:
    print(str(i, encoding='utf-8'))