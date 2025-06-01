from fastapi import Depends, FastAPI, Response, status, Query, Body
from typing import Optional
import psycopg
import os
from math import ceil
from time import time
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv
from queries import CNPJ_QUERY, FILTROS_QUERY, DATA_ABERTURA_QUERY, RAIZ_QUERY


load_dotenv()
app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
bd_nome = os.getenv('BD_NOME')
bd_usuario = os.getenv('BD_USUARIO')


def get_paginacao_template(total, pagina_atual, limite=25):
    quant_paginacoes = ceil(total / limite)
    return {
        'quantidade_total_resultados': total,
        'limite_resultados_paginacao': limite,
        'resultados_paginacao': pagina_atual,
        'quantidade_paginacoes': quant_paginacoes,
    }


def get_conn():
    conn = psycopg.connect(dbname=bd_nome, user=bd_usuario)
    try:
        yield conn
    except Exception as e:
        raise e
    finally:
        conn.close()


@app.get("/cnpj/{cnpj}", status_code=200)
def get_cnpj(cnpj: str, response: Response, conn=Depends(get_conn)):
    cnpj_base = cnpj[:8]
    cnpj_ordem = cnpj[8:12]
    cnpj_dv = cnpj[12:]
    with conn.cursor() as cursor:
        cursor.execute(CNPJ_QUERY, (cnpj_base, cnpj_ordem, cnpj_dv))
        res_json = cursor.fetchone()
    if not res_json:
        response.status_code =  status.HTTP_404_NOT_FOUND
        return {'status_code': 404, 'status_code_descricao': 'CNPJ não presente na base de dados!' }

    return res_json[0]


@app.get("/data/{data}")
def get_paginacao_data(data: str, p: int = 1, conn=Depends(get_conn)):
    """
    Consulta CNPJ's abertos em uma certa data:

    - **data**: data de abertura desejada no formado DD-MM-AAAA
    - **p**: A paginação desejada(por padrão, a primeira). A API é paginada 
    de 25 em 25 resultados atualmente.
    """

    if p < 1:
        p = 1
    offset = (p - 1) * 25
    data = '-'.join(data.split('-')[::-1])
    total = [0]
    results = []
    with conn.cursor() as cursor:
        cursor.execute(DATA_ABERTURA_QUERY, (data, offset))
        resultados = cursor.fetchall()
        cursor.execute(
                 "SELECT COUNT(*) FROM estabelecimentos WHERE (data_inicio_atividade = (%s))", 
                 (data, )
                 )
        total = cursor.fetchone()
    total = total[0]
    resultados = [res[0] for res in resultados]
    return get_paginacao_template(total, resultados)


@app.get("/cnpj_base/{cnpj_base}")
def get_paginacao_raiz(cnpj_base: str, p: int = 1, conn=Depends(get_conn)):
    """
    Consulta matrizes e filias a partir da base/raiz(8 primeiros caracteres) do CNPJ:

    - **cnpj_base**: 8 primeiros caracteres do número de inscrição do CNPJ.
    - **p**: A paginação desejada(por padrão, a primeira). A API é paginada 
    de 25 em 25 resultados atualmente.
    """

    if p < 1:
        p = 1
    offset = (p - 1) * 25
    total = [0]
    results = []
    with conn.cursor() as cursor:
        cursor.execute(RAIZ_QUERY, (cnpj_base, offset))
        resultados = cursor.fetchall()
        cursor.execute(
                "SELECT COUNT(*) FROM estabelecimentos WHERE (cnpj_base = (%s)::bpchar)", 
                 (cnpj_base, )
                 )
        total = cursor.fetchone()
    total = total[0]
    resultados = [res[0] for res in resultados]
    return get_paginacao_template(total, resultados)


@app.get("/query/")
def get_paginacao_query(
            data_abertura_min: Optional[str] = None,
            data_abertura_max: Optional[str] = None,
            razao_social: Optional[str] = None,
            situacao_cadastral: Optional[int] = None,
            capital_social_min: Optional[float] = None,
            capital_social_max: Optional[float] = None,
            uf: Optional[str] = None,
            municipio: Optional[int] = None,
            cnae_principal: Optional[str] = None,
            natureza_juridica: Optional[str] = None,
            cnpj_base: Optional[str] = None,
            p: int = 1,
            conn=Depends(get_conn)
        ):
    """
    Consultar CNPJ's abertos a partir de uma série de parâmetros possíveis de serem combinados(0 ou mais deles):

    - **data_abertura_min**: filtro por estabelecimentos abertos a partir dessa data
    - **data_abertura_max**: filtro por estabelecimentos abertos até essa data
    - **razao_social**: filtro por termo presente na razão social
    - **cnpj_base**: filtro por raiz de um cnpj(primeiros 8 caracteres), retornando a matriz e todas filiais com aquela base.
    - **situacao_cadastral**: filtro por código de situação cadastral
    - **capital_social_min**: filtro por estabelecimentos com pelo menos esse capital social.
    - **capital_social_max**: filtro por estabelecimentos com até esse capital social.
    - **uf**: filtro por estabelecimentos desse estado.
    - **municipio**: filtro por estabelecimentos desse município.
    - **cnae_principal**: filtro por estabelecimentos com esse cnae principal.
    - **natureza_juridica**: filtro por estabelecimentos com essa natureza juridica.
    - **p**: A paginação desejada(por padrão, a primeira). A API é paginada 
    de 25 em 25 resultados atualmente.
    """

    if p < 1:
        p = 1
    offset = (p - 1) * 25

    if razao_social:
        razao_social = razao_social + '%'
    parametros_filtro = {
            "offset": offset,
            "data_abertura_min": data_abertura_min,
            "data_abertura_max": data_abertura_max,
            "razao_social": razao_social,
            "situacao_cadastral": situacao_cadastral,
            "capital_social_min": capital_social_min,
            "capital_social_max": capital_social_max,
            "uf": uf,
            "municipio": municipio,
            "cnae_principal": cnae_principal,
            "natureza_juridica": natureza_juridica,
            "cnpj_base": cnpj_base
    }
    with conn.cursor() as cursor:
        cursor.execute(FILTROS_QUERY, parametros_filtro)
        results = cursor.fetchall()

    return {
            'limite_resultados_paginacao': 25,
            'paginacao_atual': p,
            'resultados_paginacao': [res[0] for res in results],
    }
