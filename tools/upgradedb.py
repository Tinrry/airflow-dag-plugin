# encoding: utf-8
# Python中默认的编码格式是ASCII 格式，在没修改编码格式时无法正确打印汉字，所以在读取中文时会报错。 解决方法为只要在文件开头加入# -*- coding: UTF-8 -*- 或者# ...
import os

from airflow import configuration
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 要在页面中设置变量
MYSQL_CONN_ID = "airflow_db"


# os.environ[MYSQL_CONN_ID] = configuration.conf.get("core", "sql_alchemy_conn")

# 在UI前端设置airflow的connection配置文件
# https://airflow.apache.org/docs/stable/howto/connection/index.html

def get_mysql():
    # os.environ[MYSQL_CONN_ID.upper()] = settings.SQL_ALCHEMY_CONN
    import mysql.connector

    conn_mysql = mysql.connector.connect(
        host="10.101.4.3",
        user="zhenghuanhuan",
        passwd="airflow",
        database="airflow"
    )
    return conn_mysql



def run_sql(sql, conn_mysql, ignore_error=False):
    mycursor = conn_mysql.cursor()
    print("sql:\n%s" % sql)
    try:
        res = mycursor.execute(sql)
    except Exception as e:
        if not ignore_error:
            raise e
        res = None
    return res

# mysql的语句不适配版本
def run_version_0_0_1(conn_mysql):
    sql = """
        CREATE TABLE IF NOT EXISTS dcmp_dag (
          id int(11) NOT NULL AUTO_INCREMENT,
          `dag_name` varchar(100) NOT NULL,
          `version` int(11) NOT NULL,
          `category` varchar(50) NOT NULL,
          `editing` tinyint(1) NOT NULL,
          `editing_user_id` int(11) DEFAULT NULL,
          `editing_user_name` varchar(100) DEFAULT NULL,
          `last_editor_user_id` int(11) DEFAULT NULL,
          `last_editor_user_name` varchar(100) DEFAULT NULL,
          `updated_at` datetime(6) NOT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `dag_name` (`dag_name`),
          KEY `category` (`category`),
          KEY `editing` (`editing`),
          KEY `updated_at` (`updated_at`)
        ) DEFAULT CHARSET=utf8mb4;
    """
    run_sql(sql, conn_mysql)
    sql_conf = """
        CREATE TABLE IF NOT EXISTS `dcmp_dag_conf` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `dag_id` int(11) NOT NULL,
          `dag_name` varchar(100) NOT NULL,
          `action` varchar(50) NOT NULL,
          `version` int(11) NOT NULL,
          `conf` text NOT NULL,
          `creator_user_id` int(11) DEFAULT NULL,
          `creator_user_name` varchar(100) DEFAULT NULL,
          `created_at` datetime(6) NOT NULL,
          PRIMARY KEY (`id`),
          KEY `dag_id` (`dag_id`),
          KEY `dag_name` (`dag_name`),
          KEY `action` (`action`),
          KEY `version` (`version`),
          KEY `created_at` (`created_at`)
        ) DEFAULT CHARSET=utf8mb4;
    """
    run_sql(sql_conf, conn_mysql)




def run_version_0_0_2(conn_mysql):
    run_sql("ALTER TABLE dcmp_dag ADD editing_start datetime(6);", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD INDEX editing_start (editing_start);", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD last_edited_at datetime(6);", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD INDEX last_edited_at (last_edited_at);", conn_mysql, ignore_error=True)


def run_version_0_1_1(conn_mysql):
    run_sql("ALTER TABLE dcmp_dag_conf CHANGE conf conf mediumtext NOT NULL;",conn_mysql)


def run_version_0_2_0(conn_mysql):
    sql = """
        CREATE TABLE IF NOT EXISTS `dcmp_user_profile` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `user_id` int(11) NOT NULL,
          `is_superuser` tinyint(1) NOT NULL,
          `is_data_profiler` tinyint(1) NOT NULL,
          `is_approver` tinyint(1) NOT NULL,
          `updated_at` datetime(6) NOT NULL,
          `created_at` datetime(6) NOT NULL,
          PRIMARY KEY (`id`),
          KEY `user_id` (`user_id`),
          KEY `is_superuser` (`is_superuser`),
          KEY `is_data_profiler` (`is_data_profiler`),
          KEY `is_approver` (`is_approver`),
          KEY `updated_at` (`updated_at`),
          KEY `created_at` (`created_at`)
        ) DEFAULT CHARSET=utf8mb4;
    """
    run_sql(sql, conn_mysql)



def run_version_0_2_1(conn_mysql):
    run_sql("ALTER TABLE dcmp_dag ADD approved_version int(11) NOT NULL;", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD INDEX approved_version (approved_version);", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD approver_user_id int(11) DEFAULT NULL;", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD approver_user_name varchar(100) DEFAULT NULL;", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD last_approved_at datetime(6);", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD INDEX last_approved_at (last_approved_at);", conn_mysql, ignore_error=True)

    run_sql("ALTER TABLE dcmp_dag_conf ADD approver_user_id int(11) DEFAULT NULL;", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag_conf ADD approver_user_name varchar(100) DEFAULT NULL;", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag_conf ADD approved_at datetime(6);", conn_mysql, ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag_conf ADD INDEX approved_at (approved_at);", conn_mysql, ignore_error=True)

    run_sql("ALTER TABLE dcmp_user_profile ADD approval_notification_emails text NOT NULL;", conn_mysql, ignore_error=True)


def main(conn_mysql):
    run_version_0_0_1(conn_mysql)
    run_version_0_0_2(conn_mysql)
    run_version_0_1_1(conn_mysql)
    run_version_0_2_0(conn_mysql)
    run_version_0_2_1(conn_mysql)


if __name__ == "__main__":
    conn_mysql = get_mysql()
    main(conn_mysql)
    conn_mysql.close()


