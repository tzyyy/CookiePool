#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
import hashlib
import threading
import pymysql
import sys
from inspect import signature
from functools import wraps


# python环境检测
s_py_version = sys.version
if s_py_version[0] == "2":
    raise SystemError("接口环境要求py3.5以上")

# 版本字符串
VERSION = (0, 1, 4)
VERSION_STRING = "%d.%d.%d" % VERSION[:3]

# Module Configuration Section begin
n_error_tmc = 100    # 调用CookiePool.__get_conn() 连接数据库并且连接数量太多导致异常时，进行重试的次数
# Module Configuration Section end


def type_check(*type_args, **type_kwargs):
    """
    利用装饰器，对方法参数进行合法性检测
    :return : 检测不通过则抛出异常
    """
    def decorate(func):
        sig = signature(func)
        bound_types = sig.bind_partial(*type_args, **type_kwargs).arguments
        # print("bound_types:",bound_types)

        @wraps(func)
        def wrapper(*args, **kwargs):
            bound_values = sig.bind(*args, **kwargs)
            for name, value in bound_values.arguments.items():
                if name in bound_types:
                    if not isinstance(value, bound_types[name]):
                        raise TypeError('参数类型错误! 参数 {} 必须为： {}'.format(name, bound_types[name]))
            return func(*args, **kwargs)
        return wrapper
    return decorate


class CID:
    """Cookie对象:作为cookies数据的标识，其中md5为唯一主键"""
    @type_check(s_province=str, s_tax=str, s_ip=str)
    def __init__(self, s_province="", s_tax="", s_ip=""):
        if "" == s_province and "" == s_tax:
            raise TypeError('参数类型错误! 参数 s_province 和 s_tax 不能同时为空')
        self._province = s_province
        self._tax = s_tax
        self._ip = s_ip
        # 可将唯一对应属性进行md5处理，__md5_deal()可接收多个参数（md5为必有属性-不可删）
        self._md5 = self.__md5_deal(s_tax)

    """注意，如果需要添加新的字表字段，此处也需修改"""
    def get_dict(self):
        """
        获取类的键和键值
        :return: 返回字典形式的类的键和键值，
                 如：{"province": "北京", "tax": "123456789616", "md5": "Hm_lvt_159f5d3f9ae2bd304b8e99efa2e9ae"}
        """
        return {"province": self._province, "tax": self._tax, "md5": self._md5, "ip": self._ip}

    def __md5_deal(self, *args):
        """
        对接受的参数进行md5处理，并返回处理值
        :return:返回md5处理值
        :rtype:str
        """
        s_txt = "".join([str(i) for i in args])
        md5_o_md = hashlib.md5()
        md5_o_md.update(s_txt.encode())
        return md5_o_md.hexdigest()


class CookiePool:
    """Cookie池对象，包括cookie信息的存取等 (使用数据库：Mysql)"""
    @type_check(s_host=str, n_port=int, s_database=str, s_user=str, s_password=str, s_table_name=str, s_charset=str)
    def __init__(self, s_host, n_port, s_database, s_user, s_password, s_table_name, cookie_id, s_charset="utf8"):
        """
        初始化数据库连接
        :param str s_host: 数据库IP地址
        :param int n_port: 数据库端口
        :param str s_database: 所使用数据库
        :param str s_user: 连接用户名
        :param str s_password: 连接密码
        :param str s_table_name: CookiePool数据所存的数据表
        :param cookie_id: CookieID()实例化对象，如CookieID(s_province="北京", s_tax="")
        """
        if n_port < 0 or n_port > 65535:
            raise TypeError('端口号范围错误！port：{},范围要求在0~65535'.format(n_port))
        self._s_host = s_host
        self._n_port = n_port
        self._s_database = s_database
        self._s_user = s_user
        self._s_password = s_password
        self._s_table_name = s_table_name
        self._s_charset = s_charset
        # 连接数据库
        self._db = self.__get_conn()
        cursor_o_flag = self._db.cursor()
        try:
            dic_data = cookie_id.get_dict()
            dic_data.pop("md5")
        except Exception as e:
            raise TypeError('CookiePool()初始化参数类型错误! 参数异常：cookie_id：{},\nerror:{}'.format(cookie_id, e))
        n_sql_add = "".join('`{}` varchar(2000) DEFAULT NULL,'.format(k) for k in dic_data.keys())
        n_sql = '''CREATE TABLE  IF NOT EXISTS `{}` ( 
                                    `md5` varchar(200) NOT NULL, {}
                                    `cookies` varchar(2000) DEFAULT NULL,
                                    `beg_time` int(20) DEFAULT NULL,
                                    `end_time` int(20) DEFAULT NULL,
                                    PRIMARY KEY (`md5`)
                                    ) DEFAULT CHARSET=utf8;'''.format(self._s_table_name, n_sql_add)
        try:
            cursor_o_flag.execute(n_sql)
        except Exception as e:
            self._db.close()
            raise TypeError('数据库CookiePool表建立失败！：n_sql：{},\nerror:{}'.format(n_sql, e))

        #   读写锁配置:一个锁对象，允许多个同时“读锁”，但是只有一个“写锁”
        self._lock = threading.Lock()
        self._read_ready = threading.Condition(self._lock)
        self._write_ready = threading.Condition(self._lock)
        self._writers = 0

    @type_check(n_alive_time=int)
    def save(self, cookie_id, s_cookies, n_alive_time=3600):
        """"
        保存cookies信息到数据库
        :param cookie_id: CookieID()实例化对象
        :param s_cookies: cookies数据
        :param n_alive_time: cookies有效时间（单位-秒）
        :return: 返回sql保存结果值
        """
        try:
            dic_data = cookie_id.get_dict()
        except:
            raise TypeError('参数类型错误!cookie_id应该为CookieID实例化对象.', cookie_id)
        try:
            self.__acquire_write()
            n_beg_time = int(time.time())
            dic_data["beg_time"], dic_data["end_time"], dic_data["cookies"] = \
                n_beg_time, n_beg_time + n_alive_time, str(s_cookies)
            n_result = self.__update(dic_data)
            if 0 == n_result:
                n_result = self.__insert(dic_data)
            else:
                pass
        except Exception as e:
            raise Exception("数据插入异常!cookie_id:{},s_cookies:{},n_alive_time:{},\nerror:{}".format(
                cookie_id, s_cookies, n_alive_time, e))
        finally:
            self.__release_write()
        return n_result

    @type_check(n_get_sum=int, n_filter_overtime=int)
    def get_by_cookieid(self, cookie_id, n_get_sum=1, n_filter_overtime=1):
        """
        获取相应条件下的cookies信息
        :param cookie_id: CookieID()实例化对象（作为查询条件），过滤掉值为""的属性条件，
                        如CookieID(s_province="北京", s_tax="")则查询s_province=="北京"的cookies数据
        :param n_get_sum: 查询数量，如果get_sum<=0则查询全部满足条件的数据
        :param n_filter_overtime: 是否过滤超时的cookies数据,默认1（即过滤）
        :return: 返回查询结果
        """
        try:
            dic_data = cookie_id.get_dict()
            dic_data.pop("md5")
        except:
            raise TypeError('参数类型错误! -- cookie_id')
        self.__acquire_read()
        """删除字典中值为空的键值对"""
        for k in list(dic_data.keys()):
            if not dic_data[k]:
                del dic_data[k]
        return self.__select(dic_data, n_get_sum, n_filter_overtime)

    def clear_all_overtime(self):
        """
        删除数据库table_name表中过时的cookies数据
        :return: 返回删除数量
        """
        self.__acquire_write()
        db = self.__get_conn()
        sql = ""
        try:
            cursor = db.cursor()
            sql = "DELETE FROM {} WHERE `end_time`<{};".format(self._s_table_name, int(time.time()))
            result = cursor.execute(sql)
            db.commit()
        except Exception as e:
            raise Exception("数据插入异常!sql:{},\nerror:{}".format(sql, e))
        finally:
            db.close()
            self.__release_write()
        return result

    def get_num(self):
        """
        获取数据库table_name表中cookies数据总数量
        :return:返回查询到的总数量
        """
        self.__acquire_read()
        db = self.__get_conn()
        cursor = db.cursor()
        s_sql = "SELECT COUNT(*) FROM {};".format(self._s_table_name)
        try:
            cursor.execute(s_sql)
            result = 0
            try:
                result = cursor.fetchone()[0]
            except:
                pass
        except Exception as e:
            raise Exception("数据库查询异常!s_sql:{},\nerror:{}".format(s_sql, e))
        finally:
            db.close()
        return result

    def __insert(self, dic_data):
        """
        数据库插入操作
        :param dic_data:插入数据
        :return:返回sql执行结果
        """
        n_sql = ""
        try:
            cursor = self._db.cursor()
            n_sql = "insert into {}(%s) values(%s)".format(self._s_table_name)
            n_sql = n_sql % (", ".join('`{}`'.format(k) for k in dic_data.keys()),
                         ', '.join('%({})s'.format(k) for k in dic_data.keys()))
            n_result = cursor.execute(n_sql, dic_data)
            self._db.commit()
            cursor.close()
            return n_result
        except Exception as e:
            raise Exception("数据插入异常!data:{},\nn_sql:{},\nerror:{}".format(
                dic_data, n_sql, e))

    def __update(self, dic_data):
        """
        数据库更新操作
        :param dic_data:更新数据
        :return:返回sql执行结果，如果无更新则返回0
        """
        s_sql = ""
        cursor = self._db.cursor()
        try:
            sql_add = " , ".join("`" + key + "`=%(" + key + ")s" for key in dic_data.keys())
            s_sql = "UPDATE {} SET {}  WHERE `md5`='{}';".format(self._s_table_name, sql_add, dic_data["md5"])
            n_result = cursor.execute(s_sql, dic_data)  # 将字典data传入
            self._db.commit()
        except Exception as e:
            raise Exception("数据插入异常!data:{},\ns_sql:{},\nerror:{}".format(
                dic_data, s_sql, e))
        finally:
            cursor.close()
        return n_result

    def __select(self, dic_data, n_get_sum=1, n_filter_overtime=1):
        """
        数据库查询操作
        :param dic_data:查询条件，格式为{"province":"浙江","tax": "1"}，表示查询province为浙江且tax为1的一条数据
        :param n_get_sum: 查询数量限制
        :param n_filter_overtime: 是否过滤超时的cookies数据,默认1（即过滤）
        :return:返回sql执行结果
        """
        now_time = int(time.time())
        db = self.__get_conn()
        n_sql = ""
        try:
            # 查询时，获取字典格式数据
            cursor = db.cursor(cursor=pymysql.cursors.DictCursor)
            n_sql_add = " AND ".join("`" + key + "`='" + value + "'" for key, value in dic_data.items())
            n_sql = "SELECT * FROM {} WHERE {} "
            n_sql = n_sql + "AND `end_time` > {} ".format(now_time) \
                if 1 == n_filter_overtime else n_sql
            n_sql = str(n_sql + "ORDER BY RAND() LIMIT {};").format(self._s_table_name, n_sql_add, n_get_sum) \
                if n_get_sum > 0 else n_sql.format(self._s_table_name, n_sql_add)
            cursor.execute(n_sql)
            res = cursor.fetchall()
        except Exception as e:
            raise Exception("数据查询异常!data:{},\nn_sql:{},\nerror:{}".format(
                dic_data, n_sql, e))
        finally:
            db.close()
        return res

    def __get_conn(self):
        """
        连接数据库的方法，当连接数量太多导致异常时，进行一定次数内的重试
        :return: 返回数据库连接
        """
        global n_error_tmc
        n_error_tmc_flag = n_error_tmc
        while True:
            try:
                db = pymysql.connect(host=self._s_host, port=self._n_port, database=self._s_database,
                                     user=self._s_user, password=self._s_password, charset=self._s_charset)
                return db
            except Exception as e:
                if "Too many connections" in str(e) and n_error_tmc_flag > 0:
                    time.sleep(0.01)
                    n_error_tmc_flag -= 1
                    # print("Too many connections,重试次数:",n_error_tmc_flag)
                else:
                    raise Exception('数据库连接异常! connect:{},\nerror:{}'.format(self.__dict__, e))

    def __acquire_read(self):
        """ 获取一个读锁。"""
        with self._read_ready:
            if self._writers > 0:
                self._read_ready.wait()

    def __acquire_write(self):
        """ 获得一个写锁。直到没有阻塞获取写入锁。"""
        with self._write_ready:
            self._writers += 1
            if self._writers > 1:
                self._write_ready.wait()

    def __release_write(self):
        """ 释放写锁。"""
        with self._write_ready:
            self._writers -= 1
            if self._writers < 0:
                self._writers = 0
            if not self._writers:
                self._read_ready.notifyAll()
            self._write_ready.notify()

    def __del__(self):
        try:
            self._db.close()
        except Exception as e:
            pass


if __name__ == '__main__':
    cookies = {'_ga': 'GA1.2.368799281.1551860816', '__gads': 'ID',
               'Hm_lvt_159f5d3f9ae2bd304b8e99efa2e9ae34': '1551884072,155193744113'}
    sTax = "123456789618"
    '''创建CookieID对象'''
    j = CID(s_province="bj", s_tax=sTax, s_ip="https://219.151.238.74:4237")

    '''创建CookiePool对象'''
    ck = CookiePool("localhost", 3306, "cookiepool", "root", "root", "cookies_pool5", j)

    '''插入Cookies对象信息(参数：j-CookieID对象;    cookies-cookies信息;  n_alive_time-cookies信息有效时间)'''
    print("插入Cookies对象信息:", ck.save(j, cookies, n_alive_time=60))

    '''查询Cookies对象信息(参数：j-CookieID对象;    1-查询数量;  0-不过滤无效cookies)'''
    print("\n查询Cookies对象信息:", ck.get_by_cookieid(CID(s_province="bj", s_tax=""), 0, 0))

    """删除数据库中过时的cookies数据"""
    print("\n删除数据库中过时的cookies数据:", ck.clear_all_overtime())

    '''获取数据库中cookies数据总数量'''
    print("\n获取数据库中cookies数据总数量:", ck.get_num())
