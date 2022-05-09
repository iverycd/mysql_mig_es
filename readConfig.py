import configparser
import os
import sys

exepath = os.getcwd()
exepath = os.path.join(exepath, "config.ini")

config = configparser.ConfigParser()
config.read(exepath)


class ReadConfig:
    def get_mysql(self, name):
        value = config.get('mysql', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
        return value

    def get_elastic(self, name):
        value = config.get('elastic', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
        return value

    def get_option(self, name):
        value = config.get('option', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
        return value


if __name__ == '__main__':
    print('path值为：', exepath)  # 测试path内容
    print('config_path', exepath + "/config.ini")  # 打印输出config_path测试内容是否正确
    print('通过config.get拿到配置文件中DATABASE的对应值\n',
          'host',ReadConfig().get_mysql('host')
          ,'database',ReadConfig().get_mysql('database'),'\n','elastic',ReadConfig().get_elastic('host'))
