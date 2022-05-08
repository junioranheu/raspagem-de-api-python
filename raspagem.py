import os
import sys
import json
import logging
import requests
import re

# import sqlserver
import mysql
import api
import removeacentos

import schedule
import time 
import datetime 

class Raspagem:
    def __init__(self, debug=True):
        self.debug = debug
        # self.db = sqlserver.SqlServer()
        self.db = mysql.MySQL()

        self.ra = removeacentos.RemoveAcentos()

        self.urlApi = 'https://spotifyapi.azurewebsites.net/api/Usuarios/todos'

    def converterNoneParaNull(self, s):
        if s is None:
            return ''

        return str(s)

    def salvarRaspagem(self, dados):
        # print(dados)

        if dados == None:
            return False

        try:
            self.db.truncate_table('Usuarios')

            for dado in dados:
                # print(dado)

                # Verificar dataOnline;
                if dado['dataOnline'] and dado['dataOnline'] != '0001-01-01T00:00:00': 
                    dataOnline = dado['dataOnline']
                else:
                    dataOnline = '0001-01-01 00:00:00.000000'

                # SQL
                strSql = """INSERT INTO Usuarios (usuarioId, nomeCompleto, email, nomeUsuarioSistema, senha,
                                                  usuarioTipoId, foto, dataCriacao, dataOnline, isAtivo, 
                                                  isPremium, isVerificado) 
                            VALUES ({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', {9}, '{10}', '{11}')
                        """.format(str(dado['usuarioId']), dado['nomeCompleto'], dado['email'], dado['nomeUsuarioSistema'], dado['senha'],
                                   str(dado['usuarioTipoId']), dado['foto'], dado['dataCriacao'], dataOnline,str(dado['isAtivo']), 
                                   str(dado['isPremium']), str(dado['isVerificado']))

                # print(strSql)
                self.db.insert_table(strSql)
                print('usuário ' + str(dado['usuarioId']) + ' salvo na tabela Usuarios com sucesso')

            return True
        except Exception as e:
            logging.exception('Erro função=salvarRaspagem: ' + str(e))
            raise Exception('Erro função=salvarRaspagem: ' + str(e))

    def corrigir(self, dados):
        try:
            dados = dados.replace(',,{',u'},{').replace(',]','}]')
            return json.loads(dados) 
        except Exception as e:
            print(str(e))        

    def consume(self, token):
        try:
            headers = {'Authorization': 'Bearer ' + token, 'X-Switch-Role': '1'}

            # =-=-=-=-=-=-=-=-=-=-=-=-=-= =-=-=-=-=-=-=-=-=-=-=-=-=-=
            response1 = requests.get(self.urlApi, headers=headers)
            # print(response1.text)

            dados_in_json1 = self.corrigir(response1.text)
            # print(dados_in_json1)

            self.salvarRaspagem(dados_in_json1)

        except Exception as e:
            logging.exception('Erro função=consume: ' + str(e))
            raise Exception('Erro função=consume: ' + str(e))

    def getToken(self):
        nomeUsuario = "adm"
        senha = "123"
        url = f"https://spotifyapi.azurewebsites.net/api/Usuarios/autenticar?nomeUsuarioSistema={nomeUsuario}&senha={senha}"
        # print(url)

        payload={}
        files=[]
        headers={}

        response = requests.request("GET", url, headers=headers, data=payload, files=files)
        token = response.text

        # print(response.text)
        # print(token)
        return token

def raspar():
    print('Iniciando raspagem agora às ' + str(datetime.datetime.now()))
    rasp = Raspagem()
    token = rasp.getToken()
    rasp.consume(token)
 
if __name__ == "__main__":
    horaSchedule = '12:45'
    print('Raspagem marcada para: ' + horaSchedule)

    schedule.every().day.at(horaSchedule).do(raspar) 
    while True:
        schedule.run_pending()
        time.sleep(1)
        print('Próxima raspagem marcada para: ' + horaSchedule)

    sys.exit(0)