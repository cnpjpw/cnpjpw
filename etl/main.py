import os
from dotenv import load_dotenv
from download import download_cnpj_zips, verificar_existencia_pasta
from parsing import gerar_csvs_utf8
from parsing import parse_csv_tabela
from load import *
import pathlib
import shutil
import json
import psycopg
from tqdm import tqdm
import logging
from utils import ler_data_json
from utils import acrescentar_mes_json
from utils import gerar_nova_data
from datetime import datetime, timezone, timedelta
import requests


def tratar_dados_abertos(nomes_csv, tipos_indices, path_dados):
    for nome in nomes_csv:
        nome_arquivo = nome + '.csv'
        path_entrada = path_dados / 'tmp'
        path_saida = path_dados / 'csv'
        parse_csv_tabela(
            indices_tipo=tipos_indices.get(nome, tipos_indices['Outros']),
            nome_arquivo=nome_arquivo,
            path_entrada=path_entrada,
            path_saida=path_saida,
        )
        (path_entrada / nome_arquivo).unlink()


def carregar_arquivos_bd(auxiliares, principais, path_dados, arq_tabela_dic, conn, faz_update, logger, staging_sufixo='staging'):
    from config import ARQ_TABELA_DIC
    logger.info('Carregando Dados Auxiliares nas Tabelas de Staging(direto para o staging2)')
    for nome in tqdm(auxiliares):
        nome_tabela = f'{ARQ_TABELA_DIC[nome]}_staging2'
        csv_path = path_dados / 'csv' / f'{nome}.csv'
        carregar_csv_banco(nome_tabela, csv_path, conn)
        csv_path.unlink()

    logger.info('Carregando Dados Principais nas Tabelas de Staging1')
    for nome in tqdm(principais):
        nome_tabela = f'{ARQ_TABELA_DIC[nome]}_{staging_sufixo}1'
        csv_path = path_dados / 'csv' / f'{nome}.csv'
        carregar_csv_banco(nome_tabela, csv_path, conn)
        csv_path.unlink()

    logger.info('Carregando Dados Principais nas Tabelas de Staging2')
    for nome in tqdm(principais):
        nome_tabela = ARQ_TABELA_DIC[nome]
        mover_entre_staging(f'{nome_tabela}_{staging_sufixo}1', f'{nome_tabela}_{staging_sufixo}2', conn)

    logger.info('Carregando Dados para Tabelas de Produção')
    for nome in tqdm(auxiliares + principais):
        nome_tabela = ARQ_TABELA_DIC[nome]
        infos_auxiliares = tabelas_infos['auxiliares']
        tabela_info = tabelas_infos.get(nome_tabela, infos_auxiliares)
        total_linhas = mover_staging_producao(
            f'{nome_tabela}_{staging_sufixo}2',
            nome_tabela,
            tabela_info['pk'],
            tabela_info['colunas'],
            conn,
            faz_update
        )
        logger.info(f'{total_linhas} linhas inseridas/atualizadas na tabela "{nome_tabela}"')


def polling_carga_mensal(bd_nome, bd_usuario, path_raiz, path_script, logger):
    from config import (
    NAO_NUMERADOS,
    NUMERADOS,
    AUXILIARES,
    PRINCIPAIS,
    NOMES_ARQUIVOS,
    TEMPLATE_URL_PASTA,
    TIPOS_INDICES,
    ARQ_TABELA_DIC,
    )
    path_json_data = path_script / "data_pasta.json"
    mes, ano = ler_data_json(path_json_data)
    url_pasta = TEMPLATE_URL_PASTA.format(mes=mes, ano=ano)

    data_atual = datetime.now(tz=timezone(timedelta(hours=-3)))
    delta_meses = (data_atual.year * 12 + data_atual.month) - (ano * 12 + mes)
    if delta_meses < 0:
        return

    logger.info(f'Verificando Existência Pasta {ano}-{str(mes).zfill(2)}')
    if (requests.get(url_pasta).status_code == 404):
        return

    logger.info('Carregando Variáveis de Configuração')

    path_dados = path_raiz / f'{str(mes).zfill(2)}-{ano}'

    logger.info('Criando diretórios(se não existirem)')
    for subdir in ['zip', 'tmp', 'csv']:
        (path_dados / subdir).mkdir(parents=True, exist_ok=True)

    logger.info('Iniciando Download dos Zips da Receita')
    download_cnpj_zips(url_pasta, NOMES_ARQUIVOS, path_dados / "zip")

    logger.info('Iniciando Extração dos Zips da Receita')
    gerar_csvs_utf8(path_dados / "zip", path_dados / "tmp", nao_numerados=NAO_NUMERADOS, numerados=NUMERADOS)

    logger.info("Iniciando Tratamento Inicial dos CSV's da Receita")
    tratar_dados_abertos(NAO_NUMERADOS + NUMERADOS, TIPOS_INDICES, path_dados)

    logger.info('Iniciando Rotinas de Carga em BD')
    with psycopg.connect(dbname=bd_nome, user=bd_usuario) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s);", (123456789,))
            carregar_arquivos_bd(AUXILIARES, PRINCIPAIS, path_dados, ARQ_TABELA_DIC, conn, True, logger)
        except Exception as e:
            print(e)
        finally:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (123456789,))
            except Exception:
                pass

    mes_antigo, ano_antigo = gerar_nova_data(mes, ano, -2)
    path_pasta_antiga = path_raiz / f'{str(mes_antigo).zfill(2)}-{ano_antigo}'
    if os.path.exists(path_pasta_antiga):
        logger.info('Removendo diretório antigo(2 meses atrás)')
        shutil.rmtree(path_pasta_antiga)

    logger.info('Modificando Mês de Download dos Dados')
    acrescentar_mes_json(path_json_data, mes, ano)
    logger.info('Carga Mensal Concluida')


if __name__ == '__main__':
    load_dotenv()
    BD_NOME = os.environ['BD_NOME']
    BD_USUARIO = os.environ['BD_USUARIO']
    PATH_RAIZ = pathlib.Path(os.environ['PATH_CNPJ_DADOS_RAIZ'])
    PATH_SCRIPT = pathlib.Path(__file__).parent
    logging.basicConfig(
        filename=PATH_SCRIPT / 'rotina_mensal.log',
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    polling_carga_mensal(BD_NOME, BD_USUARIO, PATH_RAIZ, PATH_SCRIPT, logger)

