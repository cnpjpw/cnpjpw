from dotenv import load_dotenv
import os
import pathlib
import logging
import requests
from requests.exceptions import ReadTimeout
from cnpjpw_scrapers.config import HEADERS
from cnpjpw_scrapers import gerar_novos_cnpjs
from cnpjpw_scrapers import get_cnpj_info
from cnpjpw_scrapers import parse_scrape_data
from tqdm import tqdm
from load import pegar_ultimo_cnpj_inserido
from main import carregar_arquivos_bd
import csv
import psycopg
from time import sleep
from utils import pegar_vagos_dia, pegar_matrizes_banco


def polling_carga_diaria(bd_nome, bd_usuario, path_raiz, path_script, limite_maximo, logger):
    from config import PRINCIPAIS, ARQ_TABELA_DIC
    s = requests.session()
    s.headers = HEADERS

    with psycopg.connect(dbname=bd_nome, user=bd_usuario) as conn:
        logger.info('Obtendo CNPJ mais Recente')
        primeiro_cnpj = pegar_ultimo_cnpj_inserido(conn)
        logger.info('Obtendo CNPJs deixados em raspagens anteriores')
        vagos, percentual = pegar_vagos_dia(conn)
        cnpjs_gerados = gerar_novos_cnpjs(primeiro_cnpj, limite_maximo)
        ultimo_cnpj = cnpjs_gerados[-1]
        cnpjs_banco = pegar_matrizes_banco(primeiro_cnpj[:8], ultimo_cnpj[:8], conn)
    cnpjs = [cnpj for cnpj in cnpjs_gerados if cnpj not in cnpjs_banco]
    if percentual < 1.5:
        vagos = []
    logger.info('Baixando Novas Paginas')
    download_paginas(cnpjs, s, vagos)
    logger.info('Fazendo Parsing das Paginas')
    nomes = os.listdir(path_raiz / 'tmp')
    tratar_paginas(nomes, path_raiz)

    logger.info('Iniciando Rotinas de Carga em BD')
    with psycopg.connect(dbname=bd_nome, user=bd_usuario) as conn:
        carregar_arquivos_bd([], PRINCIPAIS, path_raiz, ARQ_TABELA_DIC, conn, False, logger, staging_sufixo='staging_diario')
    logger.info('Carga Diária Concluida')


def download_paginas(cnpjs, s, vagos=[]):
    count = 0
    for cnpj in tqdm(cnpjs):
        if count == 10:
            break
        try:
            html = get_cnpj_info(cnpj, s)
        except ReadTimeout:
            continue
        if html is None:
            count += 1
            continue
        with open(PATH_RAIZ / 'tmp' / f'{cnpj}.html', 'w') as f:
            f.write(html)
        count = 0
        sleep(0.25)

    for cnpj in tqdm(vagos):
        try:
            html = get_cnpj_info(cnpj, s)
        except ReadTimeout:
            continue
        if html is None:
            continue
        with open(PATH_RAIZ / 'tmp' / f'{cnpj}.html', 'w') as f:
            f.write(html)
        sleep(0.25)


def tratar_paginas(nomes, path_raiz):
    from config import PRINCIPAIS, ARQ_TABELA_DIC
    for pagina in tqdm(nomes):
        with open(path_raiz / 'tmp' / pagina) as f:
            html = f.read()
        dados = parse_scrape_data(html)
        for nome in PRINCIPAIS:
            tabela = ARQ_TABELA_DIC[nome]
            tabela_json = dados[tabela]
            with open(path_raiz / 'csv' / f'{nome}.csv', 'a') as csvfile:
                if tabela == 'socios':
                    for socio in tabela_json:
                        writer = csv.writer(csvfile, delimiter=';')
                        writer.writerow(socio.values())
                    continue
                writer = csv.writer(csvfile, delimiter=';')
                writer.writerow(tabela_json.values())
        (path_raiz / 'tmp' / pagina).unlink()


if __name__ == '__main__':
    PATH_SCRIPT = pathlib.Path(__file__).parent
    logging.basicConfig(
        filename=PATH_SCRIPT / 'rotina_diaria.log',
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    logger.info('Carregando Variáveis e Configurações')
    load_dotenv()
    BD_NOME = os.environ['BD_NOME']
    BD_USUARIO = os.environ['BD_USUARIO']
    PATH_RAIZ = pathlib.Path(os.environ['PATH_CNPJ_DADOS_DIARIOS'])
    limite_maximo = 2000

    polling_carga_diaria(BD_NOME, BD_USUARIO, PATH_RAIZ, PATH_SCRIPT, limite_maximo, logger)

