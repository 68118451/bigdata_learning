from re import fullmatch
from pymysql import Connection  # conda install -y pymysql

## 自动生产mysql表对应的hive建表语句


# 查询MySQL表的列名、列类型和列注释
SQL_COLUMNS = '''
SELECT
  `COLUMN_NAME`     -- 列名
  ,`COLUMN_TYPE`    -- 类型
  ,`COLUMN_COMMENT` -- 列注释
FROM `information_schema`.`COLUMNS`
WHERE `TABLE_SCHEMA`='{TABLE_SCHEMA}'
  AND `TABLE_NAME`='{TABLE_NAME}'
ORDER BY `ORDINAL_POSITION`;
'''.strip().format

# 查询MySQL表的注释
SQL_COMMENT = '''
SELECT `TABLE_COMMENT`
FROM `information_schema`.`TABLES`
WHERE `TABLE_SCHEMA`='{TABLE_SCHEMA}'
  AND `TABLE_NAME`='{TABLE_NAME}';
'''.strip().format

# HIVE表前缀
HIVE_PREFIX = 'ods_'

# HIVE建表语句
HIVE_DDL = '''
CREATE EXTERNAL TABLE `{table}`(
{columns}
) COMMENT '{table_comment}'
PARTITIONED BY (`ymd` STRING COMMENT '年月日');
'''.strip().format

# MySQL原表的建表语句，用于参照
MYSQL_DDL = "SHOW CREATE TABLE `{TABLE_SCHEMA}`.`{TABLE_NAME}`".format


def column_type_mysql2hive(mysql_column_type):
    """MySQL列数据类型转成HIVE的"""
    # tinyint
    if fullmatch('^tinyint.+unsigned', mysql_column_type):
        return 'SMALLINT'
    elif fullmatch('^tinyint.*', mysql_column_type):
        return 'TINYINT'
    # smallint
    elif fullmatch('^smallint.+unsigned', mysql_column_type):
        return 'INT'
    elif fullmatch('^smallint.*', mysql_column_type):
        return 'SMALLINT'
    # mediumint
    elif fullmatch('^mediumint.*', mysql_column_type):
        return 'INT'
    # int
    elif fullmatch('^int.+unsigned', mysql_column_type):
        return 'BIGINT'
    elif fullmatch('^int.*', mysql_column_type):
        return 'INT'
    # bigint
    elif fullmatch('^bigint.+unsigned', mysql_column_type):
        # return 'STRING'
        return 'BIGINT'  # 无符号BIGINT可能会越界
    elif fullmatch('^bigint.*', mysql_column_type):
        return 'BIGINT'
    # double、float、decimal
    elif fullmatch('^double.*', mysql_column_type):
        return 'DOUBLE'
    elif fullmatch('^float.*', mysql_column_type):
        return 'FLOAT'
    elif fullmatch(r'^decimal.*', mysql_column_type):
        return mysql_column_type.replace(' unsigned', '').upper()
    # 其它
    else:
        return 'STRING'


class Mysql:
    def __init__(self, **kwargs):
        self.db = Connection(
            user=kwargs.pop('user', 'root'),
            password=kwargs.pop('password'),
            host=kwargs.pop('host', 'localhost'),
            database=kwargs.pop('database'),
            port=kwargs.pop('port', 3306),
            charset=kwargs.pop('charset', 'UTF8'),
        )
        self.cursor = self.db.cursor()

    def __del__(self):
        self.cursor.close()
        self.db.close()

    def commit(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)

    def fetchall(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()  # 有数据：tuple of tuple；无数据：()

    def get_columns(self, db, tb):
        columns = []
        for c_name, c_type, c_comment in self.fetchall(SQL_COLUMNS(TABLE_SCHEMA=db, TABLE_NAME=tb)):
            hive_type = column_type_mysql2hive(c_type)
            columns.append(f"  `{c_name}` {hive_type} COMMENT '{c_comment}',")
        return '\n'.join(columns).rstrip(',')

    def get_table_comment(self, db, tb):
        return self.fetchall(SQL_COMMENT(TABLE_SCHEMA=db, TABLE_NAME=tb))[0][0]

    def get_hive_ddl(self, db, tb, prefix='ods_mysql_', postfix='_full'):
        columns = self.get_columns(db, tb)
        comment = self.get_table_comment(db, tb)
        table = prefix + tb + postfix
        return HIVE_DDL(table=table, columns=columns, table_comment=comment)

    def get_mysql_ddl(self, db, tb):
        return self.fetchall(MYSQL_DDL(TABLE_SCHEMA=db, TABLE_NAME=tb))[0][1]


if __name__ == '__main__':
    TABLE_SCHEMA = 'azkaban'
    TABLE_NAME = 'execution_flows'
    m = Mysql(host='10.112.3.215', user='root', password='!@#456QWEasd', database='azkaban')
    print('源MySQL建表语句'.center(99, '-'))
    print(m.get_mysql_ddl(TABLE_SCHEMA, TABLE_NAME))
    print('HIVE建表语句'.center(99, '-'))
    print(m.get_hive_ddl(TABLE_SCHEMA, TABLE_NAME))
