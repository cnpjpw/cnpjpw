import os
from dotenv import load_dotenv
from download import download_cnpj_zips
from parsing import extrair_zips
from parsing import parse_csv_tabela
from load import *
import pathlib
import json
from config import TIPOS_INDICES, AUXILIARES, PRINCIPAIS, ARQ_TABELA_DIC
import psycopg
from tqdm import tqdm


def ler_data_json(path):
    with open(path, "r") as f:
        data_pasta = json.load(f)

    return (data_pasta['mes'], data_pasta['ano'])


def acrescentar_mes(path, mes, ano):
    with open(path, "w") as f:
        data_dic = {
            'mes': (mes % 12) + 1,
            'ano': ano + (mes // 12)
        }
        json.dump(data_dic, f)


def main(arqs_staging1, arqs_staging2, staging1_csv_tabelas, staging2_csv_tabelas, ARQ_TABELA_DIC, conn):
    for nome_tabela, csv_path in tqdm(staging2_csv_tabelas.items()):
        carregar_csv_banco(nome_tabela, csv_path, conn)
    for nome in tqdm(arqs_staging2):
        nome = ARQ_TABELA_DIC[nome]
        mover_staging_producao(
            f'{nome}_staging2',
            nome,
            ['codigo'],
            ['descricao'],
            conn
            )
    for nome_tabela, csv_path in tqdm(staging1_csv_tabelas.items()):
        carregar_csv_banco(nome_tabela, csv_path, conn)
    for nome in tqdm(arqs_staging1):
        nome = ARQ_TABELA_DIC[nome]
        mover_entre_staging(f'{nome}_staging1', f'{nome}_staging2', conn)
    for nome in tqdm(arqs_staging1):
        nome = ARQ_TABELA_DIC[nome]
        mover_staging_producao(
            f'{nome}_staging2',
            nome,
            tabela_infos[nome]['pk'],
            tabela_infos[nome]['colunas'],
            conn
            )




if __name__ == '__main__':
    path_raiz = pathlib.Path(os.environ['PATH_CNPJ_DADOS_RAIZ'])
    BD_NOME = os.environ['BD_NOME']
    BD_USUARIO = os.environ['BD_USUARIO']

    script_path = pathlib.Path(__file__).parent

    path_json_data = script_path / "data_pasta.json"
    mes, ano = ler_data_json(path_data_json)

    path_dados = path_raiz / f'{str(mes).zfill(2)}-{ano}'


    download_cnpj_zips(ano, mes, path_dados / "zip")
    extrair_zips(path_dados / "zip", path_dados / "tmp", auxiliares=AUXILIARES, principais=PRINCIPAIS)

    for nome in (AUXILIARES + PRINCIPAIS):
        parse_csv_tabela(
            indices_tipo=TIPOS_INDICES.get(nome, TIPOS_INDICES['Outros']),
            nome_arquivo= nome + '.csv',
            path_entrada=path_dados / 'tmp',
            path_saida=path_dados / 'csv',
        )
    arqs_staging1 = ['Simples'] + PRINCIPAIS
    arqs_staging2 = AUXILIARES[:-1]
    staging1_csv_tabelas = {f'{ARQ_TABELA_DIC[nome]}_staging1': path_dados / 'csv' / f'{nome}.csv' for nome in arqs_staging1}
    staging2_csv_tabelas = {f'{ARQ_TABELA_DIC[nome]}_staging2': path_dados / 'csv' / f'{nome}.csv' for nome in arqs_staging2}
    with psycopg.connect(dbname=BD_NOME, user=BD_USUARIO) as conn:
        main(arqs_staging1, arqs_staging2, staging1_csv_tabelas, staging2_csv_tabelas, ARQ_TABELA_DIC, conn)
    acrescentar_mes_json(path_json_data, mes, ano)
