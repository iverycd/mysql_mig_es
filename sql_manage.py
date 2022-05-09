# -*-coding:utf-8 -*-
"""
modify:"2022-04-19"
version:1.1
"""
from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import traceback
from readConfig import ReadConfig

ini_file = ReadConfig()
db_user = ini_file.get_mysql('user')
db_password = ini_file.get_mysql('passwd')
db_ip = ini_file.get_mysql('host')
db_port = ini_file.get_mysql('port')
db_name = ini_file.get_mysql('database')
es_host = ini_file.get_elastic('host')


# 用于不需要回滚和提交的操作
def find(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)  # 函数如果调用没问题就返回函数结果集，以及关闭数据库会话
        except Exception as e:
            print(traceback.format_exc())
            print(str(e))
            return traceback.format_exc()
        finally:
            self.session.close()

    return wrapper


# 对应MySQL的连接以及会话设置
class MysqlManager(object):
    def __init__(self):
        mysql_connection_string = db_user + ":" + db_password + "@" + db_ip + ":" + db_port + "/" + db_name
        self.engine = create_engine('mysql+pymysql://' + mysql_connection_string + '?charset=utf8', poolclass=QueuePool,pool_recycle=3600)
        # self.DB_Session = sessionmaker(bind=self.engine)
        # self.session = self.DB_Session()
        self.DB_Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=True, expire_on_commit=False)
        self.db = scoped_session(self.DB_Session)
        self.session = self.db()
        # 避免group_concat函数被截断
        self.session.execute('SET session group_concat_max_len=10240000;')

    @find
    def select_all_dict(self, sql, keys):
        a = self.session.execute(sql)
        # a.cursor.arraysize = 1000
        a = a.fetchall()
        lists = []
        for i in a:
            if len(keys) == len(i):  # 判断下MySQL查询结果字段是否与ES的各个字段匹配
                data_dict = {}
                for k, v in zip(keys, i):
                    data_dict[k] = v  # k是ES中列字段名称，V是MySQL查询结果集对应的列值
                lists.append(data_dict)  # 将ES的字段名称以及MySQL查询结果列值拼装好形成字典类型数据,然后再存入lists这个列表
            else:
                return False
        return lists  # 这里返回的是拼装好的ES字段以及数据格式

    # 关闭
    def close(self):
        self.session.close()
