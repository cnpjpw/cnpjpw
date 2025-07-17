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
NOMES_ARQUIVOS = AUXILIARES + [f'{p}{i}' for p in PRINCIPAIS for i in range(10)]


def extrair_arquivo_zip(zip_path, destino, novo_nome):
    with zipfile.ZipFile(zip_path, 'r') as archive:
        nome_interno = archive.namelist()[0]
        archive.extract(nome_interno, path=destino)
        os.rename(destino / nome_interno, destino / novo_nome)


def extrair_zips(path_entrada, path_saida):
    path_entrada = pathlib.Path(path_entrada)
    path_saida = pathlib.Path(path_saida)

    for nome in NOMES_ARQUIVOS:
        zip_file = path_entrada / f'{nome}.zip'
        extrair_arquivo_zip(zip_file, path_saida, f'{nome}.csv')

    for p in PRINCIPAIS:
        destino = path_saida / f'{p}.csv'
        with open(destino, 'wb') as wfd:
            for i in range(10):
                arquivo_particionado = path_saida / f'{p}{i}.csv'
                with open(arquivo_particionado, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd)
            arquivo_particionado.unlink()



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
        line[i] = (line[i] == 'S')

    line = [limpar_valor(val) for val in line]
    return line, ultimo_id


def parse_csv_tabela(indices_tipo, nome_arquivo, path_entrada, path_saida, total_linhas=100_000_000):
    entrada = path_entrada / nome_arquivo
    saida = path_saida / nome_arquivo

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

    path = pathlib.Path(os.getenv('PATH_CNPJ_DATA'))
    ano_pasta = '06-2025'
    base_path = path / ano_pasta

    path_zips = (base_path / 'zip')
    path_csv_bruto = (base_path / 'tmp')
    path_csv_tratado = (base_path / 'csv')

    path_csv_bruto.mkdir(parents=True, exist_ok=True)
    path_csv_tratado.mkdir(parents=True, exist_ok=True)

    for nome in (PRINCIPAIS + AUXILIARES):
        print(nome)
        parse_csv_tabela(
            indices_tipo=TIPOS_INDICES.get(nome, TIPOS_INDICES['Outros']),
            nome_arquivo= nome + '.csv',
            path_entrada=path_csv_bruto,
            path_saida=path_csv_tratado,
        )



