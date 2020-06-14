import os
import redis

from redis_ac_keywords import RedisACKeywords

def print_used_memory(client):
    for k, v in client.info().items():
        if k.find('memory') != -1:
            print(k, v)

if __name__ == '__main__':
    '''
    used_memory_peak_human 24.56M
    used_memory 661168
    used_memory_lua 20480
    used_memory_rss 2916352
    used_memory_human 645.67K
    used_memory_peak 25749008
    {'keywords': 20161, 'nodes': 67140}
    used_memory_peak_human 24.56M
    used_memory 25749208
    used_memory_lua 20480
    used_memory_rss 27140096
    used_memory_human 24.56M
    used_memory_peak 25749008
    '''
    client = redis.Redis(host='127.0.0.1', db=12)
    print_used_memory(client)

    f = open(os.path.expanduser('keywords.txt'))

    keywords = RedisACKeywords(host='127.0.0.1', name='benchmark')
    for line in f.readlines():
        keyword = line.strip()
        keywords.add(keyword)
    print(keywords.info())
    print_used_memory(client)
