import os
from dotenv import load_dotenv
from download import download_cnpj_zips
from parsing import extrair_zips
from parsing import parse_csv_tabela
from load import *
import pathlib
import json
from config import TIPOS_INDICES, NAO_NUMERADOS, NUMERADOS, ARQ_TABELA_DIC
import psycopg
from tqdm import tqdm


def ler_data_json(path):
    with open(path, "r") as f:
        data_pasta = json.load(f)

    return (data_pasta['mes'], data_pasta['ano'])


def acrescentar_mes_json(path, mes, ano):
    with open(path, "w") as f:
        data_dic = {
            'mes': (mes % 12) + 1,
            'ano': ano + (mes // 12)
        }
        json.dump(data_dic, f)


def main(principais, auxiliares, path_dados, arq_tabela_dic, conn):
    for nome in tqdm(auxiliares):
        nome_tabela = f'{arq_tabela_dic[nome]}_staging2'
        csv_path = path_dados / f'{nome}.csv'
        carregar_csv_banco(nome_tabela, csv_path, conn)

    for nome in tqdm(auxiliares):
        nome_tabela = arq_tabela_dic[nome]
        mover_staging_producao(
            f'{nome}_staging2',
            nome,
            ['codigo'],
            ['descricao'],
            conn
        )

    for nome in tqdm(principais):
        nome_tabela = f'{arq_tabela_dic[nome]}_staging1'
        csv_path = path_dados / f'{nome}.csv'
        carregar_csv_banco(nome_tabela, csv_path, conn)

    for nome in tqdm(principais):
        nome = arq_tabela_dic[nome]
        mover_entre_staging(f'{nome}_staging1', f'{nome}_staging2', conn)

    for nome in tqdm(principais):
        nome = arq_tabela_dic[nome]
        mover_staging_producao(
            f'{nome}_staging2',
            nome,
            tabela_infos[nome]['pk'],
            tabela_infos[nome]['colunas'],
            conn
        )


if __name__ == '__main__':
    BD_NOME = os.environ['BD_NOME']
    BD_USUARIO = os.environ['BD_USUARIO']
    PATH_RAIZ = pathlib.Path(os.environ['PATH_CNPJ_DADOS_RAIZ'])
    PATH_SCRIPT = pathlib.Path(__file__).parent

    path_json_data = PATH_SCRIPT / "data_pasta.json"
    mes, ano = ler_data_json(path_json_data)
    path_dados = PATH_RAIZ / f'{str(mes).zfill(2)}-{ano}'

    download_cnpj_zips(ano, mes, path_dados / "zip")
    extrair_zips(path_dados / "zip", path_dados / "tmp", nao_numerados=NAO_NUMERADOS, numerados=NUMERADOS)

    for nome in (NAO_NUMERADOS + NUMERADOS):
        parse_csv_tabela(
            indices_tipo=TIPOS_INDICES.get(nome, TIPOS_INDICES['Outros']),
            nome_arquivo= nome + '.csv',
            path_entrada=path_dados / 'tmp',
            path_saida=path_dados / 'csv',
        )
    with psycopg.connect(dbname=BD_NOME, user=BD_USUARIO) as conn:
        main(PRINCIPAIS, AUXILIARES, path_dados / 'csv', ARQ_TABELA_DIC, conn)
    acrescentar_mes_json(path_json_data, mes, ano)
