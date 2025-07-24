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

path_raiz = pathlib.Path(os.environ['PATH_CNPJ_DADOS_RAIZ'])
BD_NOME = os.environ['BD_NOME']
BD_USUARIO = os.environ['BD_USUARIO']

script_path = pathlib.Path(__file__).parent
with open(script_path / "data_pasta.json", "r") as f:
    data_pasta = json.load(f)

MES = data_pasta["mes"]
ANO = data_pasta["ano"]

path_dados = path_raiz / f'{str(MES).zfill(2)}-{ANO}'


download_cnpj_zips(ANO, MES, path_dados / "zip")
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
data_pasta['mes'] += 1
if data_pasta['mes'] == 13:
    data_pasta['mes'] = 1
    data_pasta['ano'] += 1

with open(script_path / "data_pasta.json", "w") as f:
    json.dump(data_pasta, f)

