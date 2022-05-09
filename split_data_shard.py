# -*- coding: utf-8 -*-
"""
        init_list为初始化的列表，childern_list_len初始化列表中的几个数据组成一个小列表
        把一个大list按照切片大小分割，比如5个元素list[a,b,c,d,e],按照切片大小2切割后，就是[[a,b],[c,d],[e]]
        :param init_list:
        :param childern_list_len:
        :return:
"""


class data_split:
    def list_of_groups(init_list, childern_list_len):
        list_of_group = zip(*(iter(init_list),) * childern_list_len)
        end_list = [list(i) for i in list_of_group]
        count = len(init_list) % childern_list_len
        end_list.append(init_list[-count:]) if count != 0 else end_list
        return end_list
