import csv
import zipfile
import os
import shutil
import pathlib
from tqdm import tqdm
from config import TIPOS_INDICES
from dotenv import load_dotenv

load_dotenv()

AUXILIARES = ['Cnaes', 'Motivos', 'Municipios', 'Naturezas', 'Paises', 'Qualificacoes', 'Simples']
PRINCIPAIS = ['Empresas', 'Estabelecimentos', 'Socios']


def criar_pastas(base_path):
    (base_path / 'tmp').mkdir(parents=True, exist_ok=True)
    (base_path / 'csv').mkdir(parents=True, exist_ok=True)


def extrair_arquivo_zip(zip_path, destino, novo_nome):
    with zipfile.ZipFile(zip_path, 'r') as archive:
        nome_interno = archive.namelist()[0]
        archive.extract(nome_interno, path=destino)
        os.rename(destino / nome_interno, destino / novo_nome)


def extrair_zips(path):
    base_path = pathlib.Path(path)
    criar_pastas(base_path)

    for nome in AUXILIARES + [f'{p}{i}' for p in PRINCIPAIS for i in range(10)]:
        zip_file = base_path / f'{nome}.zip'
        destino = base_path / ('csv' if nome in AUXILIARES else 'tmp')
        extrair_arquivo_zip(zip_file, destino, f'{nome}.csv')

    # Junta arquivos particionados em único CSV
    for p in PRINCIPAIS:
        destino = base_path / 'csv' / f'{p}.csv'
        with open(destino, 'wb') as wfd:
            for i in range(10):
                parte = base_path / 'tmp' / f'{p}{i}.csv'
                with open(parte, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd)

    shutil.rmtree(base_path / 'tmp')


def limpar_valor(val):
    if isinstance(val, str):
        val = val.replace('\x00', '')
    return None if val == '' else val


def formatar_linha(line, indices_tipo, ultimo_id):
    if indices_tipo['id'] and all(line[i] == ultimo_id[i] for i in indices_tipo['id']):
        return None, ultimo_id

    ultimo_id = [line[i] for i in indices_tipo['id']]

    for i in indices_tipo['array']:
        line[i] = '{' + line[i] + '}'

    for i in indices_tipo['date']:
        data = line[i]
        line[i] = None if len(data) != 8 or int(data) == 0 else data

    for i in indices_tipo['numeric']:
        line[i] = line[i].replace(',', '.')

    for i in indices_tipo['bool']:
        line[i] = line[i] == 'S'

    line = [limpar_valor(val) for val in line]
    return line, ultimo_id


def parse_csv_tabela(indices_tipo, nome_arquivo, path, total_linhas=100_000_000):
    base_path = pathlib.Path(path)
    entrada = base_path / nome_arquivo
    saida = base_path / 'temp.csv'

    with open(entrada, 'r', encoding='latin-1') as f_in, \
         open(saida, 'w', encoding='latin-1', newline='') as f_out:

        leitor = csv.reader(f_in, delimiter=';')
        escritor = csv.writer(f_out, delimiter=';')

        ultimo_id = ['' for _ in indices_tipo['id']]
        for linha in tqdm(leitor, total=total_linhas):
            linha_formatada, ultimo_id = formatar_linha(linha, indices_tipo, ultimo_id)
            if linha_formatada:
                escritor.writerow(linha_formatada)


if __name__ == '__main__':
    parse_csv_tabela(
        indices_tipo=TIPOS_INDICES['socios'],
        nome_arquivo='Socios.csv',
        path=os.getenv('PATH_CNPJ_DATA'),
        # total_linhas=26_000_000  # descomente se quiser ajustar
    )

