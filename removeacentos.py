import unicodedata
import re

class RemoveAcentos:
    def __init__(self):
        pass
    
    def removerAcentos(self, palavra):
        # Unicode normalize transforma um caracter em seu equivalente em latin.
        nfkd = unicodedata.normalize('NFKD', palavra)
        palavraSemAcento = u"".join([c for c in nfkd if not unicodedata.combining(c)])

        # Usa expressão regular para retornar a palavra apenas com números, letras e espaço
        return re.sub('[^a-zA-Z0-9 \\\]', '', palavraSemAcento)

if __name__=="__main__":
    ra = RemoveAcentos()
    print(ra.removerAcentos('87Hkjfd´dà'))