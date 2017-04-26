# -*- coding: utf-8 -*-
# !/usr/bin/env python

'''
self.table_name为Redis中的一个key
2017/4/17 修改pop
'''

import json
import redis

class RedisClient(object):
  """
  Reids client
  """

  def __init__(self, name, host, port):
    """
    init
    :param table_name:
    :param host:
    :param port:
    :return:
    """
    self.table_name = name
    self.__conn = redis.Redis(host=host, port=port, db=0)

  def get(self):
    """
    get random result
    :return:
    """
    return self.__conn.srandmember(table_name=self.table_name)

  def put(self, value):
    """
    put an item
    :param value:
    :return:
    """
    if self.table_name == "useful_proxy_queue":
      print("useful_proxy_queue %s" % value)
    elif self.table_name == "raw_proxy":
      print("raw_proxy %s" % value)
    else:
      print("%s, %s" % (self.table_name, value))
    proxy = json.dump(value, ensure_ascii=False).encode('utf-8') \
        if isinstance(value, (dict, list)) else value.encode("utf-8")
    return self.__conn.sadd(self.table_name, proxy)

  def pop(self):
    """
    pop an item
    :return:
    """
    proxy = self.__conn.spop(self.table_name)
    return proxy.decode("utf-8") if proxy is not None else proxy

  def delete(self, value):
    """
    delete an item
    :param key:
    :return:
    """
    self.__conn.srem(self.table_name, value)

  def getAll(self):
    return self.__conn.smembers(self.table_name)

  def get_status(self):
    return self.__conn.scard(self.table_name)

  def changeTable(self, table_name):
    self.table_name = table_name


if __name__ == '__main__':
  redis_con = RedisClient('proxy', 'localhost', 6379)
  # redis_con.put('abc')
  # redis_con.put('123')
  # redis_con.put('123.115.235.221:8800')
  # print redis_con.getAll()
  # redis_con.delete('abc')
  # print redis_con.getAll()
  # redis_con.pop()
  # print redis_con.getAll()
  redis_con.changeTable('raw_proxy')
  #redis_con.put('132.112.43.221:8888')
  # redis_con.changeTable('proxy')
  print(redis_con.get_status())
  print(redis_con.getAll())
  redis_con.changeTable('useful_proxy_queue')
  print(redis_con.get_status())
  print(redis_con.getAll())
