import psycopg2
from psycopg2.extras import Json
import json
import logging


class PostgresHandler():
    def __init__(self, db_name, db_user, db_password, db_host):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.connect(self.db_name, self.db_user,
                     self.db_password, self.db_host)
        self.logger = logging.getLogger('postgres_handler')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        bf = logging.Formatter(
            '{asctime} {name} {levelname:8s} {message}', style='{')
        handler.setFormatter(bf)
        self.logger.addHandler(handler)

    def connect(self, db_name, db_user, db_password, db_host):
        try:
            self.connection = psycopg2.connect(
                database=db_name, user=db_user, password=db_password, host=db_host)
            self.cursor = self.connection.cursor()
        except Exception as exc:
            self.logger.error('postgres connection error: {}'.format(exc))

    def close(self):
        self.cursor.close()
        self.connection.close()

    # where rows like (row1, row2, row3) with brackets
    # where values is (some_data1, some_data2, some_data3)
    def insert(self, table_name, rows, values):
        try:
            if self.connection.closed != 0:
                self.logger.info('postgres connection lost, try to reconnect')
                self.connect(self.db_name, self.db_user,
                             self.db_password, self.db_host)
            rows = self.striper(rows)
            self.cursor.execute(
                "INSERT INTO {} {} VALUES {}".format(table_name, rows, values))
            self.connection.commit()
            return True
        except Exception as exc:
            self.logger.error('postgres insertion error: {}'.format(exc))

    def update(self, table_name, rows, values, row_cond, value_cond):
        try:
            if self.connection.closed != 0:
                self.logger.info('postgres connection lost, try to reconnect')
                self.connect(self.db_name, self.db_user,
                             self.db_password, self.db_host)
            rows = self.striper(rows)
            self.cursor.execute("UPDATE {} SET {} = {} WHERE {} = '{}'".format(
                table_name, rows, values, row_cond, value_cond))
            self.connection.commit()
            return True
        except Exception as exc:
            self.logger.error('postgres update error: {}'.format(exc))

    # you can't use == with jsonb... but we do not need it yet =)
    # def delete(self, table_name, row_name, where_equal):
    #     # rows = self.striper(rows)
    #     self.cursor.execute("DELETE FROM {} WHERE {} == '{}'".format(table_name, row_name, where_equal) )
    #     self.connection.commit()

    def execute(self, execute_string):
        try:
            if self.connection.closed != 0:
                self.logger.info('postgres connection lost, try to reconnect')
                self.connect(self.db_name, self.db_user,
                             self.db_password, self.db_host)
            self.cursor.execute(execute_string)
            result = self.cursor.fetchall()
            # self.connection.commit()
            return result
        except Exception as exc:
            self.logger.error('postgres exec error: {}'.format(exc))

    def striper(self, data):
        return '{}'.format(data).replace("'", "")


def main():

    PH = PostgresHandler('device_config_backup',
                         'alexander', '123456', 'localhost')
    PH.close()
    PH.insert('device_config_operator_deviceconfig',
              ('device_config', 'device_id'), ('{"some_data1111":22}', 3))
    PH.update('device_config_operator_device',
                (
                    'device_name'
                ),
                (
                    "'{}'".format('ff_f_dedvice_name')
                ),
                'id', 4)

    # PH.delete('device_config_operator_deviceconfig', 'device_config', '{"some_data1111":22}')


if __name__ == '__main__':
    main()
