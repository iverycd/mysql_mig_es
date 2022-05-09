# -*- coding: utf-8 -*-
# 读取data_dict_map.txt，获取MySQL表的各个列字段名称
# 2022-04-27 调整列名统一为小写
from readConfig import ReadConfig
import pymysql

ini_file = ReadConfig()
db_user = ini_file.get_mysql('user')
db_password = ini_file.get_mysql('passwd')
db_ip = ini_file.get_mysql('host')
db_port = ini_file.get_mysql('port')
db_name = ini_file.get_mysql('database')
es_host = ini_file.get_elastic('host')


class mysql_col:
    def fetch_col(self, mysql_table_name):
        mysql_conn = pymysql.connect(host=db_ip, port=int(db_port), user=db_user, password=db_password,
                                     database=db_name)
        mysql_cur = mysql_conn.cursor()
        mysql_col = []
        mysql_table_count = 0
        try:
            mysql_cur.execute("""select * from %s where 1=0 limit 1""" % mysql_table_name)
            desc = mysql_cur.description
            for field in desc:
                mysql_col.append(field[0].lower())
            mysql_col = tuple(mysql_col)
            mysql_cur.execute("""select count(*) from %s""" % mysql_table_name)
            mysql_table_count = mysql_cur.fetchone()[0]
        except Exception as e:
            print(e,'获取MySQL表结构列名称失败，请检查表是否存在!')
            filename = 'run_result.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('表' + mysql_table_name + '不存在，数据传输终止' + '\n')
            f.close()
        return mysql_col,mysql_table_count
