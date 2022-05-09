# -*- coding: utf-8 -*-
# es创建索引以及bulk写入
import ast
import datetime
import time
from collections import deque
from elasticsearch import helpers
from readConfig import ReadConfig
from elasticsearch import Elasticsearch

ini_file = ReadConfig()
es_host = ini_file.get_elastic('host')


class Esmethod(object):
    def __init__(self):
        self.es = Elasticsearch(es_host)

# 创建索引
    def create_index(self,mysql_table_name, es_index, mysql_cursor):
        es_col_sql = """select concat('{',group_concat(concat('"',lower(column_name),'"',':','{"type" : "', case when data_type in ('varchar','char','tinytext','text','longtext','tinyblob','blob','longblob') then 'text' 
        when data_type in ('date','datetime','year','time','timestamp') then 'date' 
        when data_type in ('tinyint','smallint','mediumint','int','bigint') then 'long'
        when data_type in ('double','float','decimal') then 'float'
        else 'text' end,'"',',"fields" : {"keyword" : {"type" : "keyword", "ignore_above" : 256} } }')),'}')
        from information_schema.COLUMNS where table_schema=database() and table_name='%s'""" % mysql_table_name
        # sql拼接es表结构
        try:
            result = mysql_cursor.execute(es_col_sql)
            es_col_value = result.fetchone()[0]
        except Exception as e:
            print('获取表结构失败：', mysql_table_name, e)
        if es_col_value:
            # 将字符串转成dict
            es_col_value_dict = ast.literal_eval(es_col_value)
            # es的表结构
            maps = {
                "properties": es_col_value_dict
            }
            # es索引index的设置
            sets = {
                "index": {
                    "number_of_shards": "1",
                    "number_of_replicas": "0"
                }
            }
            # 创建index
            try:
                self.es.indices.create(index=es_index, mappings=maps, settings=sets)
            except Exception as e:
                print('创建索引', es_index, '失败', e)

# bulk写入数据
    def bulk_parallel_write(self,actions=[], split_index=0, es_bulk_chunk_size=500, es_thread_count=8):
        filename = 'es_write_time.log'
        f = open(filename, 'a', encoding='utf-8')
        f.write(str(time.time()) + '\n')
        f.close()
        print(datetime.datetime.now(), 'thread ', split_index, 'is running parallel_bulk')
        try:
            deque(helpers.parallel_bulk(client=self.es, actions=actions[split_index], chunk_size=es_bulk_chunk_size,
                                        thread_count=es_thread_count, request_timeout=60), maxlen=0)
            f = open(filename, 'a', encoding='utf-8')
            f.write(str(time.time()) + '\n')
            f.close()
        except Exception as e:
            print('ES写入失败', e)
            f = open(filename, 'a', encoding='utf-8')
            f.write(str(time.time()) + '\n')
            f.close()
            filename = 'error.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write(str(e) + '\n')
            f.close()
