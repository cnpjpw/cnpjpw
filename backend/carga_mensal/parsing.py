import csv
from tqdm import tqdm
from config import TIPOS_INDICES
import zipfile
import os
import shutil
import pathlib


def extrair_zips(path):
    auxiliares = ['Cnaes', 'Motivos', 'Municipios', 'Naturezas', 'Paises', 'Qualificacoes', 'Simples']
    principais = ['Empresas', 'Estabelecimentos', 'Socios']
    principais_arquivos = [principal + str(num) for num in range(10) for principal in principais]
    arquivos_nomes = auxiliares + principais_arquivos

    pathlib.Path(path + 'tmp/').mkdir(parents=True, exist_ok=True)
    pathlib.Path(path + 'csv/').mkdir(parents=True, exist_ok=True)

    for nome_arquivo in arquivos_nomes:
        with zipfile.ZipFile(path + nome_arquivo + '.zip', mode="r") as archive:
            name = archive.namelist()[0]
            pasta_extracao = 'tmp/'
            if nome_arquivo in auxiliares:
                pasta_extracao = 'csv/'
            path_extracao = path + pasta_extracao
            archive.extract(name, path=path_extracao)
            os.rename(path_extracao + name, path_extracao + nome_arquivo + '.csv')

    arquivos_particionados = list(map(lambda x: [x + str(i) for i in range(10)], principais))

    for principal in principais:
        with open(path + 'csv/' + principal + '.csv', 'wb') as wfd:
            for f in [path + 'tmp/' + principal + str(i) + '.csv' for i in range(10)]:
                with open(f,'rb') as fd:
                    shutil.copyfileobj(fd, wfd)
    shutil.rmtree(path + 'tmp/')

def parse_csv_tabela(indices_tipo, nome_arq, path, total_linhas=100_000_000):
    with open(path + nome_arq, 'r', encoding='latin-1') as f_in, \
         open(path + 'temp.csv', 'w', encoding='latin-1', newline='') as f_out:

        line_reader = csv.reader(f_in, delimiter=';')
        line_writer = csv.writer(f_out, delimiter=';')

        ultimo_id = ['' for i in range(len(indices_tipo['id']))]
        for k, line in enumerate(tqdm(line_reader, total=total_linhas)):
            if len(indices_tipo['id']) != 0 and all(line[i] == ultimo_id[i] for i in indices_tipo['id']):
                continue
            ultimo_id = [line[i] for i in indices_tipo['id']]

            for i in indices_tipo['array']:
                line[i] = '{' + line[i] + '}'
            for i in indices_tipo['date']:
                #Exemplo: 20250430
                data = line[i]
                if len(data) != 8:
                    line[i] = None
                elif int(data) == 0:
                    line[i] = None
            for i in indices_tipo['numeric']:
                    line[i] = line[i].replace(',', '.')
            for i in indices_tipo['bool']:
                val = line[i]
                line[i] = True if val == 'S' else False
            for i in range(len(line)):
                val = line[i]
                if type(val) == str:
                    val = val.replace('\x00', '')
                    line[i] = val
                if val == '':
                    line[i] = None

            line_writer.writerow(line)

#generalizar logica
if __name__ == '__main__':
    indices_tipo = TIPOS_INDICES['socios']
    nome_arq = 'Socios.csv'
    path = os.load_dotenv('PATH_CNPJ_DATA')

    #print(extrair_zips(path))
    #parse_csv_tabela(indices_tipo, nome_arq, path + 'csv/')
