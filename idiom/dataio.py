# -*- coding: utf-8 -*-
from mysql.connector.pooling import MySQLConnectionPool
import logging
import os
import codecs


logger = logging.getLogger(__name__)

class TableHandler(object):
    def __init__(self, config, pool_size=5):
        self._database = {
            'host': config.get('host'),
            'port': config.get('port'),
            'user': config.get('user'),
            'password': config.get('password'),
            'database': config.get('database'),
        }
        self._pool = MySQLConnectionPool(pool_name=None, pool_size=pool_size, **self._database)

    def get_fields(self, table, auto_key=None):
        sql = 'select column_name from information_schema.columns where table_name = "{0}" order by ordinal_position'.format(table)
        try:
            conn = self._pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            res = cursor.fetchall()  # [(col1,), (col2,)]
            conn.close()
            return [r[0] for r in res if r[0] != auto_key]
        except:
            logger.error('数据库连接错误')
            return

    def count_data(self, table):
        sql = 'select count(*) from {0}'.format(table)
        try:
            conn = self._pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            res = cursor.fetchall()  # [(1000,)]
            conn.close()
            return res[0][0]
        except:
            logger.error('数据库连接错误')
            return

    def read_data(self, table, fields=None, auto_key=None, batch_size=2000, cursor_from=0, cursor_to=None):
        fields = self.get_fields(table, auto_key) if fields is None else fields
        sql = 'select {0} from {1}'.format(', '.join(fields), table)
        conn = None
        try:
            conn = self._pool.get_connection()
            cursor = conn.cursor()
            start = cursor_from
            end = self.count_data(table) if cursor_to is None else cursor_to
            if auto_key is None:
                cursor_sql = lambda offset, batch: sql + ' limit {0}, {1}'.format(offset, batch)  # 用limit全表扫描，随offset增加速度变慢
            else:
                cursor_sql = lambda offset, batch: sql + ' where {0}>{1} and {0}<={2}'.format(auto_key, offset, offset+batch)  # 用自增列索引查询，效率更高
            while start < end:
                size = min(batch_size, end - start)
                tmp_sql = cursor_sql(start, size)
                cursor.execute(tmp_sql)
                tmp_data = cursor.fetchall()
                size = len(tmp_data)
                start += size
                logger.info('读取记录数：{0}'.format(size))
                yield tmp_data
        except:
            logger.error('数据库连接错误')
        finally:
            if conn is not None:
                conn.close()
            return

    def write_data(self, data, table, fields=None, auto_key=None, batch_size=2000):
        fields = self.get_fields(table, auto_key) if fields is None else fields
        sql = 'insert into {0} ({1}) values ({2})'.format(table, ', '.join(fields), ', '.join(['%s'] * len(fields)))
        conn = None
        try:
            conn = self._pool.get_connection()
            cursor = conn.cursor()
            start = 0
            end = len(data)
            logger.info('写入记录数：{0}'.format(end))
            while start < end:
                tmp_data = data[start : start+batch_size]
                cursor.executemany(sql, tmp_data)
                start += batch_size
            conn.commit()
        except BaseException:
            logger.exception('数据库连接错误')
        finally:
            if conn is not None:
                conn.close()
            return


def txt_to_table(file_path, db_config, table, auto_key=None):
    handler = TableHandler(db_config)
    if os.path.isdir(file_path):
        file_list = os.listdir(file_path)
        for file in file_list:
            if os.path.isdir(file):
                txt_to_table(file, db_config, table, auto_key)
            else:
                file = os.path.join(file_path, file)
                with open(file, 'r', encoding='utf8')as f:
                    data_list = list()
                    for line in f.readlines():
                        data_list.append(line.split('\t'))
                    print(len(data_list))
                    handler.write_data(data=data_list, table=table, auto_key=auto_key)


if __name__ == '__main__':
    # configs = {
    #         'host': 'localhost',
    #         'port': '3306',
    #         'user': 'user',
    #         'password': '88888888',
    #         'database': 'webfiles',
    #         }
    # table = 'web_file_test'
    # fields = ['col1', 'col2']
    # data = [tuple(range(k, k+2)) for k in range(999)]
    # handler = TableHandler(configs)
    # handler.write_data(data=data, table=table, auto_key='auto_id', batch_size=100)
    # print(handler.get_fields(table))
    # print(handler.count_data(table))
    # for tmp in handler.read_data(table=table, batch_size=200, auto_key='auto_id', cursor_from=500, cursor_to=600):
    #     print(len(tmp))
    #     print(tmp)

    db_config = {
        'host': 'localhost',
        'port': '3306',
        'user': 'user',
        'password': '88888888',
        'database': 'webfiles',
    }
    table = 'web_file_idiom'
    auto_key = 'num_id'
    txt_to_table('res', db_config, table, auto_key)
