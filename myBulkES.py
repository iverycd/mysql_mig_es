# -*- coding: utf-8 -*-
"""
modify:"2022-04-29"
version:1.3.6
desc:优化代码，增加会话的参数
"""
import ast
import concurrent
import os
from sql_manage import MysqlManager
import traceback
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pymysql
from mysql_table_col import mysql_col
from readConfig import ReadConfig
from split_data_shard import data_split
from es_method import Esmethod
import run_info


# 读取ini配置文件
ini_file = ReadConfig()


# 负责把MySQL第一页分页记录写入到ES
def first_page_write(es_write_thread, split_actions, actions, es_bulk_chunk_size, es_thread_count):
    es_method_obj = Esmethod()
    # 先查询源库MySQL第一页的数据，插入到ES
    try:
        print('ES bulk实际运行线程数:', es_write_thread)
        # 以下准备bulk写入第一页数据
        es_wrt_start_time = time.time()
        print("ES批量写入开始: ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(es_wrt_start_time)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=es_write_thread) as executor:
            task = {executor.submit(es_method_obj.bulk_parallel_write, split_actions, v_index, es_bulk_chunk_size, es_thread_count): v_index for v_index in range(es_write_thread)}
            for future in concurrent.futures.as_completed(task):
                task_name = task[future]
                try:
                    data = future.done()
                except Exception as e:
                    print(e)
        es_wrt_end_time = time.time()
        print('bulk写操作获取到的batch大小:', len(actions))
        # 第一页记录写入到ES耗时
        es_bulk_1 = round((es_wrt_end_time - es_wrt_start_time), 2)
        print("ES批量写入结束: ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(es_wrt_end_time)), "\nES写入耗时:", es_bulk_1)
    except Exception as e:
        print('处理第一页记录异常', e)


# 除第一页分页记录之外，多进程读，多线程写，多进程读取MySQL表分页，然后同时使用多线程parallel_bulk写入到ES
def parallel_read_write(split_page_index, page_size, pk_key, mysql_table_name, sqldatas, dict_set, es_index,
                        bulk_batch_size, es_bulk_chunk_size, es_thread_count, v_index,
                        es_write_thread):
    """
        MySQL同时查询分页记录，按照总分页记录进行切割分片，每个分片是分页查询读取的范围，比如有50页记录
        如果要同时进行2个分页查询，那么读进程1可以读取0到24的分页记录，读进程2读取25-49的分页记录，
        每个读进程对数据进行包装形成ES格式类型数据，封装的数据再进行分片，使用多线程把分片的记录调用ES的parallel_bulk写入到ES
        :param split_page_index:分页记录总数，分片成[[0,1,2],[3,4,5]]
        :param page_size:分页batch size
        :param pk_key:主键列名
        :param mysql_table_name:表名
        :param sqldatas:sql查询游标结果集
        :param dict_set:ES列名
        :param es_index:ES index名称
        :param bulk_batch_size:设定ES bulk每个线程的batch大小
        :param es_bulk_chunk_size: bulk的chunk大小
        :param es_thread_count: parallel_bulk参数，线程数
        :param v_index:此方法的进程号
        :param es_write_thread:ES调用bulk写的线程数，为实际运行线程数
        :return:
    """
    # 每个进程生成一个连接
    mm = MysqlManager()
    es_method_obj = Esmethod()
    sql_exec_total = 0
    mysql_cursor = mm.session
    object_data = data_split
    print('当前进程process ', v_index, '正在读取分片:', split_page_index[v_index])
    # 分派每个进程读取对应的分片，比如总共有50页记录，total_page_num_list[0]就是从0到24的记录，total_page_num_list[1]就是从25到49的记录
    for page_index in split_page_index[v_index]:
        cur_start_page = page_index * page_size  # cur_start_page从0开始
        try:
            fetch_id_start = time.time()
            # 查询上一页数据最大的一条记录（id或者guid）
            last_reocrd_id = mysql_cursor.execute(
                """select max(%s) from (select %s from %s order by %s  limit %s,%s) aa""" % (
                    pk_key, pk_key, mysql_table_name, pk_key, cur_start_page, page_size))
            p1_sql = """select max(%s) from (select %s from %s order by %s  limit %s,%s) aa""" % (
                pk_key, pk_key, mysql_table_name, pk_key, cur_start_page, page_size)
            print('当前进程', v_index, '正在获取上一条分页最大id：', p1_sql)
            max_guid = last_reocrd_id.fetchone()[0]
            fetch_id_end = time.time()
            # 计算每次查询上一页最大id，sql执行时间
            fetch_id_spent = round((fetch_id_end - fetch_id_start), 2)
            if max_guid is not None:
                actions = []
                # 根据上面获取到的id，再去查询分页记录
                select_sql_p2 = sqldatas + """  where %s> '%s' limit %s """ % (pk_key, max_guid, page_size)
                select_start_time = time.time()
                db_data_list = mm.select_all_dict(select_sql_p2, dict_set)
                select_end_time = time.time()
                # 计算MySQL在每页，分页查询执行时间
                sql_spent_time = round((select_end_time - select_start_time), 2)
                print('查询语句:', select_sql_p2, '查询结束,耗时:', sql_spent_time, 'process ', v_index)
                # 分页查询sql执行的总时长=查询上一条最大id执行耗时+每页查询耗时
                sql_exec_total = sql_exec_total + sql_spent_time + fetch_id_spent
                if db_data_list:
                    for db_data in db_data_list:  # 这里使用上面获取到的ES数据，再一次进行拼装ES类型数据格式
                        action = {
                            "_index": es_index,
                            # "_type": es_type,  # 在ES 8.1.0 无需指定_type类型
                            "@timestamp": time.strftime("%Y-%m-%dT%H:%M:%S",
                                                        time.localtime(time.time())),
                            "_source": db_data
                        }
                        # 将主键与ES的_id关联
                        es_id = db_data.get(pk_key, "")
                        if es_id:
                            action["_id"] = es_id
                        # 将拼装好的数据放进list中
                        actions.append(action)
            # 上面拼装好ES的格式之后，下面进行parallel_bulk写入到ES
            if len(actions) > 0:
                split_actions = object_data.list_of_groups(actions, bulk_batch_size)
                print('ES bulk写同时运行线程数:', len(split_actions))
                es_wrt_start_time = time.time()
                print("ES批量写入开始: ", '当前读进程process number:', v_index,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(es_wrt_start_time)))
                # 以下多线程调用parallel_bulk同时写数据到ES
                with concurrent.futures.ThreadPoolExecutor(max_workers=es_write_thread) as executor:
                    task = {executor.submit(es_method_obj.bulk_parallel_write, split_actions, v_index, es_bulk_chunk_size, es_thread_count): v_index for v_index in range(es_write_thread)}
                    for future in concurrent.futures.as_completed(task):
                        task_name = task[future]
                        try:
                            data = future.done()
                        except Exception as e:
                            print(e)
                es_wrt_end_time = time.time()
                es_spent_time = round((es_wrt_end_time - es_wrt_start_time), 2)
                print("ES批量写入结束: ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(es_wrt_end_time)), "\nES写入耗时:",
                      es_spent_time, '读进程:', v_index, '获取结束')
        except Exception as e:
            print('读进程number', v_index, "执行失败", e)


class TongBu(object):
    def __init__(self):
        try:
            run_info.print_database_info()
            # 连接mysql
            self.mm = MysqlManager()
            # 在写数据前，清空掉es每次写日志的文件
            path_file = ['es_write_time.log','run_result.log','error.log']
            for log in path_file:
                if os.path.exists(log):
                    os.remove(log)
            self.object_data = data_split
            self.tongbu_obj = Esmethod()
        except:
            print(traceback.format_exc())

    def mysql_transfer_es(self, page_size, es_bulk_chunk_size, es_thread_count, bulk_split_thread,
                          mysql_fenye_parallel_run):
        try:
            mysql_cursor = self.mm.session
            mysql_object = mysql_col()
            # es的ip和端口
            es_host = ini_file.get_elastic('host')
            print("es_host:", es_host)
            # 同步开始时间
            start_time = time.time()
            print("start..............", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))
            es_write_thread = 0
            statement_filespath = []
            # 获得所有sql文件list
            # statement_filespath = mysql.get("statement_filespath", [])
            map_file = 'data_dict_map.txt'
            with open(map_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for data in lines:
                    # 从data_dict_map.txt文件获取的字符串转为dict数据格式
                    data_map = ast.literal_eval(data)
                    statement_filespath.append(data_map)
            # 从data_dict_map.txt文件读取index以及表名
            if statement_filespath:
                for filepath in statement_filespath:
                    # mysql表名
                    mysql_table_name = filepath.get("table_name")
                    # es的索引
                    es_index = filepath.get("index")
                    # es的type
                    es_type = filepath.get("type")
                    # 先创建ES的表结构
                    self.tongbu_obj.create_index(mysql_table_name, es_index, mysql_cursor)
                    # 获取MySQL表结构，列名
                    dict_set,mysql_table_count = mysql_object.fetch_col(mysql_table_name)
                    # dict_set = db_field.get(es_type)
                    print('dict_set:', dict_set)
                    # 访问mysql，得到一个list，元素都是字典，键是字段名，值是数据
                    # db_data_list = self.mm.select_all_dict(sqldatas, dict_set)  # 获取所有数据拼接成ES格式类型
                    print('MySQL Table Name:', mysql_table_name)
                    print("表", mysql_table_name, '总行数:', mysql_table_count)
                    # 根据表总行数做分页判断
                    if mysql_table_count > 0:
                        try:
                            fetch_pk_key = mysql_cursor.execute(
                                """SELECT lower(column_name) FROM information_schema.key_column_usage WHERE constraint_name='PRIMARY' AND table_schema IN (SELECT DATABASE ()) AND table_name='%s' """ % mysql_table_name)
                            # 判断是否有主键
                            is_pk_key = fetch_pk_key.rowcount
                            # print('is_pk_key:', is_pk_key)
                        except Exception as e:
                            print('查询MySQL主键字段异常', e)
                            continue
                        # 以下对源MySQL数据库进行分页查询
                        # 自动计算总共有几页
                        total_page_num = round((mysql_table_count + page_size - 1) / page_size)
                        print('分页计算出总共有', total_page_num, '页')
                        pk_key = ''
                        if is_pk_key == 0:
                            run_result_info = '表'+ mysql_table_name + '无主键，本次数据传输忽略'
                            print(run_result_info)
                            filename = 'run_result.log'
                            f = open(filename, 'a', encoding='utf-8')
                            f.write(run_result_info + '\n')
                            f.close()
                        if is_pk_key == 1:  # 表有主键
                            # 获取主键字段名称
                            try:
                                pk_key = fetch_pk_key.fetchone()[0]
                                print('主键字段:', pk_key)
                            except Exception as e:
                                print(e)
                            # 默认查表所有字段
                            sqldatas = "select * from " + mysql_table_name
                            select_sql_p1 = sqldatas + """ limit %s,%s """ % (0, page_size)
                            select_start_time = time.time()
                            db_data_list = self.mm.select_all_dict(select_sql_p1, dict_set)
                            select_end_time = time.time()
                            sql_spent_time = round((select_end_time - select_start_time), 2)
                            print('查询语句:', select_sql_p1, '查询结束,耗时:', sql_spent_time)
                            if db_data_list:
                                actions = []  # 在拼装json数据前先置空list
                                # 将数据拼装成es的格式
                                # print("ES拼装格式开始：", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                                for db_data in db_data_list:  # 这里使用上面获取到的ES数据，再一次进行拼装ES类型数据格式
                                    action = {
                                        "_index": es_index,
                                        # "_type": es_type,  # 在ES 8.1.0 无需指定_type类型
                                        "@timestamp": time.strftime("%Y-%m-%dT%H:%M:%S",
                                                                    time.localtime(time.time())),
                                        "_source": db_data
                                    }
                                    # 表主键关联ES的_id
                                    es_id = db_data.get(pk_key, "")
                                    if es_id:
                                        action["_id"] = es_id
                                    # 将拼装好的数据放进list中
                                    actions.append(action)  # 这里是MySQL数据与ES字段名进行组合，拼装成ES类型数据
                            # 上面拼装好ES的格式之后，处理读进程以及写线程
                            if len(actions) > 0:
                                # 把总分页记录一一记录到一个list
                                split_page_index = []
                                for i in range(total_page_num):
                                    split_page_index.append(i)
                                # total_page_num是主函数计算出分页的总记录，
                                # mysql_fenye_pallel_run是自己设定的读取进程数，并非实际运行的进程数
                                # 如果设定的进程数比分页总数大，就限定读取进程数为2
                                if mysql_fenye_parallel_run > total_page_num:
                                    mysql_fenye_parallel_run = 2
                                multi_thread_input = int(total_page_num / mysql_fenye_parallel_run)
                                # 如果只有1页记录，就规避掉整除为0的情况
                                if multi_thread_input == 0:
                                    multi_thread_input = 1
                                # 以下对MySQL分页查询页记录进行分片，用于给每个进程读取一个分片,
                                # 比如6页，分成2批，第一批是[0,1,2],第二批是[3,4,5]
                                split_page_index = self.object_data.list_of_groups(split_page_index, multi_thread_input)
                                # 下面是计算MySQL实际能运行分页查询的进程数
                                mysql_read_thread = len(split_page_index)
                                # 写的进程，以下是计算es bulk每个线程的batch size
                                bulk_batch_size = int(page_size / bulk_split_thread)
                                split_actions = self.object_data.list_of_groups(actions, bulk_batch_size)
                                # es_write_thread是es bulk实际能同时运行的线程数
                                es_write_thread = len(split_actions)
                            # 第一页数据写入到ES
                            first_page_write(es_write_thread, split_actions, actions,
                                             es_bulk_chunk_size, es_thread_count)
                            # 除第一页记录，剩余结果，先多进程查分页结果，最后多线程全部写入到ES
                            with concurrent.futures.ProcessPoolExecutor(max_workers=mysql_read_thread) as executor:
                                task = {
                                    executor.submit(parallel_read_write, split_page_index, page_size, pk_key,
                                                    mysql_table_name, sqldatas, dict_set, es_index,
                                                    bulk_batch_size, es_bulk_chunk_size, es_thread_count, v_index, es_write_thread, es_host): v_index
                                    for v_index in range(mysql_read_thread)}
                                for future in concurrent.futures.as_completed(task):
                                    task_name = task[future]
                                    try:
                                        data = future.done()
                                    except Exception as e:
                                        print(e)
                            es_write_total_time = es_write_elapsed_time()
                            run_result_info = '表' + mysql_table_name + '写入到ES INDEX ' + es_index + ' 已完成，耗时:' \
                        + str(es_write_total_time) + '秒 ES写入平均速率:' + str(round(mysql_table_count / es_write_total_time))
                            filename = 'run_result.log'
                            f = open(filename, 'a', encoding='utf-8')
                            f.write(run_result_info + '\n')
                            f.close()
        except Exception as e:
            print(traceback.format_exc())
        else:
            end_time = time.time()
            print("end...................", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
            print('数据迁移摘要：\n')
            log = 'run_result.log'
            with open(log, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for info in lines:
                    print(info)
        finally:
            # 报错就关闭数据库
            self.mm.close()


def es_write_elapsed_time():
    """
    ES bulk每次开始、结束记录时间戳到log文件，最后计算第一行、最后一行
    取差值，即ES写入耗时
    """
    file_name = 'es_write_time.log'
    with open(file_name, 'r', encoding='utf-8') as f:  # 打开文件
        lines = f.readlines()  # 读取所有行
        es_write_begin = lines[0]  # 取第一行
        es_write_end = lines[-1]  # 取最后一行
        es_write_elapsed = round(float(es_write_end) - float(es_write_begin), 2)
    return es_write_elapsed


def main():
    tb = TongBu()
    mysql_pagesize = int(ini_file.get_option('page_size'))
    bulk_size = int(ini_file.get_option('es_bulk_chunk_size'))
    thread_count = int(ini_file.get_option('es_thread_count'))
    write_thread = int(ini_file.get_option('bulk_split_thread'))
    read_thread = int(ini_file.get_option('mysql_fenye_parallel_run'))
    tb.mysql_transfer_es(page_size=mysql_pagesize, es_bulk_chunk_size=bulk_size, es_thread_count=thread_count, bulk_split_thread=write_thread,
                         mysql_fenye_parallel_run=read_thread)


if __name__ == '__main__':
    main()
