import csv
import zipfile
import os
import shutil
import pathlib
from tqdm import tqdm


def extrair_arquivo_zip(path_zip, destino):
    with zipfile.ZipFile(path_zip, 'r') as archive:
        nome_interno = archive.namelist()[0]
        archive.extract(nome_interno, path=destino)
    return nome_interno


def copiar_latin1_utf8(path_latin, path_utf):
    with open(path_latin, 'r', encoding='latin-1') as fd, \
         open(path_utf, 'w', encoding='utf-8') as wfd:
            shutil.copyfileobj(fd, wfd)


def gerar_csv_unificado_numerados(nome, path_entrada, path_saida):
    destino = path_saida / f'{nome}.csv'
    with open(destino, 'wb') as wfd:
        for i in range(10):
            arquivo_particionado = path_entrada / f'{nome}{i}.csv'
            with open(arquivo_particionado, 'rb') as fd:
                shutil.copyfileobj(fd, wfd)


def gerar_csvs_utf8(path_entrada, path_saida, nao_numerados=[], numerados=[]):
    nomes_arqs = nao_numerados + [f'{f}{i}' for f in numerados for i in range(10)]

    for nome in nomes_arqs:
        path_zip = path_entrada / f'{nome}.zip'
        nome_interno = extrair_arquivo_zip(path_zip, path_saida)
        copiar_latin1_utf8(path_saida / nome_interno, path_saida / f'{nome}.csv')
        (path_saida / nome_interno).unlink()

    for nome in numerados:
        gerar_csv_unificado_numerados(nome, path_saida, path_saida)
        for i in range(10):
            (path_saida / f'{nome}{i}.csv').unlink()

def limpar_valor(val):
    if isinstance(val, str):
        val = val.replace('\x00', '')
        val = ' '.join(val.split())
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

    with open(entrada, 'r', encoding='UTF-8') as f_in, \
         open(saida, 'w', encoding='UTF-8', newline='') as f_out:

        leitor = csv.reader(f_in, delimiter=';')
        escritor = csv.writer(f_out, delimiter=';')

        ultimo_id = ['' for _ in indices_tipo['id']]
        for linha in tqdm(leitor, total=total_linhas):
            linha_formatada, ultimo_id = formatar_linha(linha, indices_tipo, ultimo_id)
            if linha_formatada:
                escritor.writerow(linha_formatada)


