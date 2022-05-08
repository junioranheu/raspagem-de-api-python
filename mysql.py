import pymysql

import logging
import time
import shutil
import os

import fileinput

class MySQL:
    def __init__(self):
        try:
            self.database = 'teste_raspagem'

            #self.server = 'SRVDHTBD\DESENV'        
            #self.username = 'xxx'
            #self.password = 'xxx'

            self.server = 'server_aqui_xxx'        
            self.username = 'teste_raspagem'
            self.password = 'ProjetoPython19@'

            #self.tmpdir_sql = r'\\SrvPBD\SELECOES_BI\temp_carga'
            self.tmpdir_sql = r'D:\\Downloads'

            self.connect = pymysql.connect( host=self.server, user=self.username, passwd=self.password, db=self.database )
            self.cursor = self.connect.cursor()
        except Exception as e:
            logging.debug('Erro SQL função=init: ' + str(e))
            raise Exception('Erro SQL função=init: ' + str(e))

    def select_table_one(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            return self.cursor.fetchone()
        except Exception as e:
            logging.debug('Erro SQL função=select_table_one: ' + str(e))
            raise Exception('Erro SQL função=select_table_one: ' + str(e))

    def select_table_count(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            cont = self.cursor.fetchone()

            if cont[0] > 0:
                return True
            else:
                return False
        except Exception as e:
            logging.debug('Erro SQL função=select_table_count: ' + str(e))
            raise Exception('Erro SQL função=select_table_count: ' + str(e))

    def select_table(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            return self.cursor.fetchall()
        except Exception as e:
            logging.debug('Erro SQL função=select_table: ' + str(e))
            raise Exception('Erro SQL função=select_table: ' + str(e))

    def update_table(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SQL função=update_table: ' + str(e))
            raise Exception('Erro SQL função=update_table: ' + str(e))         

    def insert_table(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SQL função=insert_table: ' + str(e))
            raise Exception('Erro SQL função=insert_table: ' + str(e))   

    def insert_multiple_table(self, sqlcommand, params):
        try:
            t0 = time.time()
            self.cursor.executemany(sqlcommand, params)
            self.connect.commit()
            print(f'{time.time() - t0:.1f} seconds')
        except Exception as e:
            logging.debug('Erro SQL função=insert_multiple_table: ' + str(e))
            raise Exception('Erro SQL função=insert_multiple_table: ' + str(e))

    def truncate_table(self, tabela):
        try:
            strSql = "TRUNCATE TABLE {0}".format(tabela)
            self.cursor.execute(strSql)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SQL função=truncate_table: ' + str(e))
            raise Exception('Erro SQL função=truncate_table: ' + str(e))

    def delete_table(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SQL função=delete_table: ' + str(e))
            raise Exception('Erro SQL função=delete_table: ' + str(e))

    def exec_procedure_simples(self, procedure):
        try:
            self.cursor.execute('exec {0}'.format(procedure))
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SQL função=exec_procedure_simples: ' + str(e))
            raise Exception('Erro SQL função=exec_procedure_simples: ' + str(e))

    def bulk_table(self, table, filename, debug=False, firstrow=2, fieldtermitor=';', rowterminator='\\n'):
        try:
            base = os.path.basename(filename)
            newfile = '{0}\{1}'.format(self.tmpdir_sql, base)
            
            if debug == False:
                shutil.copy(filename, newfile) 

                with fileinput.FileInput(newfile, inplace=True, backup='.bak') as file:
                    for line in file:
                        print(line.replace('"', ''), end='')

            sqlcommand = """
                BULK INSERT {0}
                FROM '{1}' WITH (
                    FIRSTROW={2},
                    FIELDTERMINATOR='{3}',
                    ROWTERMINATOR='{4}'
                    );
                """.format(table, newfile, firstrow, fieldtermitor, rowterminator)

            self.cursor.execute(sqlcommand)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SQL função=bulk_table: ' + str(e))
            raise Exception('Erro SQL função=bulk_table: ' + str(e))

    def close_connect(self):
        try:
            self.connect.close()
        except Exception as e:
            logging.debug('Erro SQL função=close_connect: ' + str(e))
            raise Exception('Erro SQL função=close_connect: ' + str(e))

if __name__=="__main__":
    db = SQL()

