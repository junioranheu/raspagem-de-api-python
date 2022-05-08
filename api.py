import json
import requests
import logging

class API:
    def __init__(self, url):
        self.url = url

    def consume(self):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}            
            response = requests.get(self.url, headers=headers)

            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            logging.debug('Erro ao consumir API função=consume: '+ str(e))
            raise Exception('Erro ao consumir API função=consume')

    def consume_get(self):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}            
            response = requests.get(self.url, headers=headers)

            if response.status_code == 200:
                return response
            else:
                return None
        except Exception as e:
            logging.debug('Erro ao consumir API função=consume: '+ str(e))
            raise Exception('Erro ao consumir API função=consume')        

if __name__=="__main__":
    pass