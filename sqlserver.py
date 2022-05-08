import pyodbc

import logging
import time
import shutil
import os

import fileinput

class SqlServer:
    def __init__(self, database):
        try:
            self.database = database

            self.server = 'SRVDHTBD\DESENV'        
            self.username = 'xxx'
            self.password = 'xxx'

            #self.tmpdir_sql = r'\\Srvdhtbd\temp_carga'
            self.tmpdir_sql = r'\\SrvPBD\SELECOES_BI\temp_carga'

            self.connect = pyodbc.connect('Driver={SQL Server Native Client 11.0};Server='+self.server+';Database='+self.database+';UID='+self.username+';PWD='+self.password+';')
            self.cursor = self.connect.cursor()
        except Exception as e:
            logging.debug('Erro SqlServer função=init: ' + str(e))
            raise Exception('Erro SqlServer função=init: ' + str(e))

    def select_table_one(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            return self.cursor.fetchone()
        except Exception as e:
            logging.debug('Erro SqlServer função=select_table_one: ' + str(e))
            raise Exception('Erro SqlServer função=select_table_one: ' + str(e))

    def select_table_count(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            cont = self.cursor.fetchone()

            if cont[0] > 0:
                return True
            else:
                return False
        except Exception as e:
            logging.debug('Erro SqlServer função=select_table_count: ' + str(e))
            raise Exception('Erro SqlServer função=select_table_count: ' + str(e))

    def select_table(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            return self.cursor.fetchall()
        except Exception as e:
            logging.debug('Erro SqlServer função=select_table: ' + str(e))
            raise Exception('Erro SqlServer função=select_table: ' + str(e))

    def update_table(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SqlServer função=update_table: ' + str(e))
            raise Exception('Erro SqlServer função=update_table: ' + str(e))         

    def insert_table(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SqlServer função=insert_table: ' + str(e))
            raise Exception('Erro SqlServer função=insert_table: ' + str(e))   

    def insert_multiple_table(self, sqlcommand, params):
        try:
            t0 = time.time()
            self.cursor.executemany(sqlcommand, params)
            self.connect.commit()
            print(f'{time.time() - t0:.1f} seconds')
        except Exception as e:
            logging.debug('Erro SqlServer função=insert_multiple_table: ' + str(e))
            raise Exception('Erro SqlServer função=insert_multiple_table: ' + str(e))

    def truncate_table(self, tabela):
        try:
            strSql = "TRUNCATE TABLE {0}".format(tabela)
            self.cursor.execute(strSql)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SqlServer função=truncate_table: ' + str(e))
            raise Exception('Erro SqlServer função=truncate_table: ' + str(e))

    def delete_table(self, sqlcommand):
        try:
            self.cursor.execute(sqlcommand)
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SqlServer função=delete_table: ' + str(e))
            raise Exception('Erro SqlServer função=delete_table: ' + str(e))

    def exec_procedure_simples(self, procedure):
        try:
            self.cursor.execute('exec {0}'.format(procedure))
            self.connect.commit()
        except Exception as e:
            logging.debug('Erro SqlServer função=exec_procedure_simples: ' + str(e))
            raise Exception('Erro SqlServer função=exec_procedure_simples: ' + str(e))

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
            logging.debug('Erro SqlServer função=bulk_table: ' + str(e))
            raise Exception('Erro SqlServer função=bulk_table: ' + str(e))

    def close_connect(self):
        try:
            self.connect.close()
        except Exception as e:
            logging.debug('Erro SqlServer função=close_connect: ' + str(e))
            raise Exception('Erro SqlServer função=close_connect: ' + str(e))

if __name__=="__main__":
    db = SqlServer("Nome_da_base_de_dados")

