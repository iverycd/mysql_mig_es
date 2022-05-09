# -*- coding:utf-8 -*-
import time

from prettytable import PrettyTable


def print_database_info():
    # print author info
    titile = 'Mysql Transfer ElasticSearch Tool'
    k = PrettyTable(field_names=[titile])
    k.align[titile] = "l"  # 以name字段左对齐
    k.padding_width = 1  # 填充宽度
    k.add_row(["Support Database: MySQL 8,Elastic 8"])
    k.add_row(["Tool Version: 1.3.5"])
    k.add_row(["Descriptor: A tool easy migrate Mysql to ElasticSearch"])
    k.add_row(["Release Date: 2022-04-28"])
    k.add_row(["Powered By: Infrastructure Research Center of Epoint"])
    print(k.get_string(sortby=titile, reversesort=False))
    time.sleep(5)
    # 自带样式打印，参数还可以选择“DEFAULT”、“PLAIN_COLUMNS”
    # x.set_style(MSWORD_FRIENDLY)
    # print(x)


if __name__ == '__main__':
    print_database_info()
