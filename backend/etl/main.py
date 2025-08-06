import os
from dotenv import load_dotenv
from download import download_cnpj_zips
from parsing import extrair_zips
from parsing import parse_csv_tabela
from load import *
import pathlib
import json
from config import *
import psycopg
from tqdm import tqdm
import logging
from utils import ler_data_json
from utils import acrescentar_mes_json


def polling_carga_mensal():
    PATH_SCRIPT = pathlib.Path(__file__).parent

    logging.basicConfig(
        filename=PATH_SCRIPT / 'rotina_mensal.log',
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    logger.info('Carregando Variáveis de Ambiente e Configurações')
    load_dotenv()
    BD_NOME = os.environ['BD_NOME']
    BD_USUARIO = os.environ['BD_USUARIO']
    PATH_RAIZ = pathlib.Path(os.environ['PATH_CNPJ_DADOS_RAIZ'])

    path_json_data = PATH_SCRIPT / "data_pasta.json"
    mes, ano = ler_data_json(path_json_data)
    path_dados = PATH_RAIZ / f'{str(mes).zfill(2)}-{ano}'

    logger.info('Criando diretórios(se não existirem)')
    (path_dados / 'zip').mkdir(parents=True, exist_ok=True)
    (path_dados / 'tmp').mkdir(parents=True, exist_ok=True)
    (path_dados / 'csv').mkdir(parents=True, exist_ok=True)

    logger.info('Iniciando Download dos Zips da Receita')
    download_cnpj_zips(ano, mes, path_dados / "zip")
    logger.info('Iniciando Extração dos Zips da Receita')
    extrair_zips(path_dados / "zip", path_dados / "tmp", nao_numerados=NAO_NUMERADOS, numerados=NUMERADOS)

    logger.info("Iniciando Tratamento Inicial dos CSV's da Receita")
    for nome in (NAO_NUMERADOS + NUMERADOS):
        nome_arquivo = nome + '.csv'
        path_entrada = path_dados / 'tmp'
        path_saida = path_dados / 'csv'
        parse_csv_tabela(
            indices_tipo=TIPOS_INDICES.get(nome, TIPOS_INDICES['Outros']),
            nome_arquivo=nome_arquivo,
            path_entrada=path_entrada,
            path_saida=path_saida,
        )
        (path_entrada / nome_arquivo).unlink()
    logger.info('Iniciando Rotinas de Carga em BD')
    with psycopg.connect(dbname=BD_NOME, user=BD_USUARIO) as conn:
        carregar_arquivos_bd(AUXILIARES, PRINCIPAIS, path_dados, ARQ_TABELA_DIC, conn, True)
    logger.info('Modificando Mês de Download dos Dados')
    acrescentar_mes_json(path_json_data, mes, ano)
    logger.info('Carga Mensal Concluida')


def polling_carga_diaria():
    logging.basicConfig(
        filename=PATH_SCRIPT / 'rotina_diaria.log',
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    logger.info('Carregando Variáveis de Ambiente e Configurações')
    load_dotenv()
    BD_NOME = os.environ['BD_NOME']
    BD_USUARIO = os.environ['BD_USUARIO']

    path_dados = ''

    logger.info('Iniciando Rotinas de Carga em BD')
    with psycopg.connect(dbname=BD_NOME, user=BD_USUARIO) as conn:
        carregar_arquivos_bd([], PRINCIPAIS, path_dados, ARQ_TABELA_DIC, conn, True)
    logger.info('Carga Mensal Concluida')


def carregar_arquivos_bd(auxiliares, principais, path_dados, arq_tabela_dic, conn, faz_update):
        logger.info('Carregando Dados Auxiliares nas Tabelas de Staging(direto para o staging2)')
        for nome in tqdm(auxiliares):
            nome_tabela = f'{ARQ_TABELA_DIC[nome]}_staging2'
            csv_path = path_dados / 'csv' / f'{nome}.csv'
            carregar_csv_banco(nome_tabela, csv_path, conn)
            csv_path.unlink()

        logger.info('Carregando Dados Principais nas Tabelas de Staging1')
        for nome in tqdm(principais):
            nome_tabela = f'{ARQ_TABELA_DIC[nome]}_staging1'
            csv_path = path_dados / 'csv' / f'{nome}.csv'
            carregar_csv_banco(nome_tabela, csv_path, conn)
            csv_path.unlink()

        logger.info('Carregando Dados Principais nas Tabelas de Staging2')
        for nome in tqdm(principais):
            nome_tabela = ARQ_TABELA_DIC[nome]
            mover_entre_staging(f'{nome_tabela}_staging1', f'{nome_tabela}_staging2', conn)

        logger.info('Carregando Dados para Tabelas de Produção')
        for nome in tqdm(auxiliares + principais):
            nome_tabela = ARQ_TABELA_DIC[nome]
            infos_auxiliares = tabelas_infos['auxiliares']
            tabela_info = tabelas_infos.get(nome_tabela, infos_auxiliares)
            mover_staging_producao(
                f'{nome_tabela}_staging2',
                nome_tabela,
                tabela_info['pk'],
                tabela_info['colunas'],
                conn,
                faz_update
            )



if __name__ == '__main__':
    polling_carga_mensal()

