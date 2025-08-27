from fastapi import Depends, FastAPI, Response, status, Query, Body
from typing import Optional, Annotated
import psycopg
from psycopg import ClientCursor
import os
from math import ceil
from time import time
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv
from queries import CNPJ_QUERY, RAZAO_QUERY, RAIZ_QUERY, DATA_ABERTURA_QUERY, COUNT_DATA_QUERY, COUNT_RAIZ_QUERY, COUNT_RAZAO_QUERY, SOCIOS_QUERY, get_busca_difusa_query
import unicodedata

load_dotenv()
app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
bd_nome = os.getenv('BD_NOME')
bd_usuario = os.getenv('BD_USUARIO')


def get_paginacao_template(pagina_atual, limite=25):
    return {
        'limite_resultados_paginacao': limite,
        'resultados_paginacao': pagina_atual,
    }

def normalizar_razao(razao: str):
    forma_nfkd = unicodedata.normalize('NFKD', razao)
    normalizado = ''.join([c for c in forma_nfkd if not unicodedata.combining(c)])
    razao = normalizado
    razao = razao.upper()
    razao = ' '.join(razao.split())
    return razao

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


@app.get("/razao_social/{razao_social}")
def get_paginacao_razao_social(razao_social: str, cursor: Optional[str] = None, conn=Depends(get_conn)):
    """
    Consulta matrizes e filias a partir do nome empresarial(razão social):

    - **razao_social**: filtro por termo presente no começo(somente no começo por enquanto) da razão social
    - **cursor**: se especificado, serão exibidos apenas resultados após o cnpj_base passado ao paramêtro 'cursor'.

    exibindo de 25 em 25 resultados atualmente.
    """
    razao_social = normalizar_razao(razao_social)
    results = []
    parametros = { 'razao_social': razao_social, 'cursor': cursor }
    with conn.cursor() as c:
        c.execute(RAZAO_QUERY, parametros)
        resultados = c.fetchall()
    resultados = [res[0] for res in resultados]
    return get_paginacao_template(resultados)


@app.get("/cnpj_base/{cnpj_base}")
def get_paginacao_raiz(cnpj_base: str, cursor: Optional[str] = None, conn=Depends(get_conn)):
    """
    Consulta matrizes e filias a partir da base/raiz(8 primeiros caracteres) do CNPJ:

    - **cnpj_base**: 8 primeiros caracteres do número de inscrição do CNPJ.
    - **p**: A paginação desejada(por padrão, a primeira). A API é paginada
    - **cursor**: se especificado, serão exibidos apenas resultados após o cnpj_ordem passado ao paramêtro 'cursor'.

    exibindo de 25 em 25 resultados atualmente.
    """

    total = [0]
    results = []
    parametros = { 'cnpj_base': cnpj_base, 'cursor': cursor }
    with conn.cursor() as cursor:
        cursor.execute(RAIZ_QUERY, parametros)
        resultados = cursor.fetchall()
    resultados = [res[0] for res in resultados]
    return get_paginacao_template(resultados)


@app.get("/data/{data}")
def get_paginacao_data(data: str, cursor: Annotated[Optional[str], Query(min_length=14, max_length=14)] = None, conn=Depends(get_conn)):
    """
    Consulta CNPJ's abertos em uma certa data:

    - **data**: data de abertura desejada no formado DD-MM-AAAA
    - **cursor**: se especificado, serão exibidos apenas resultados após o cnpj passado ao paramêtro 'cursor'.

    exibindo de 25 em 25 resultados atualmente.
    """

    data = '-'.join(data.split('-')[::-1])

    parametros = {
            'data_inicio_atividade' : data,
            'cnpj_base': None,
            'cnpj_ordem': None,
            'cnpj_dv': None
            }

    if cursor:
        parametros['cnpj_base'] = cursor[:8]
        parametros['cnpj_ordem'] = cursor[8:12]
        parametros['cnpj_dv'] = cursor[12:]

    total = [0]
    results = []
    with conn.cursor() as cursor:
        cursor.execute(DATA_ABERTURA_QUERY, parametros)
        resultados = cursor.fetchall()
    resultados = [res[0] for res in resultados]
    return get_paginacao_template(resultados)


@app.get("/socio/{doc}")
def get_paginacao_socio(doc: str, cursor: Optional[str] = None, conn=Depends(get_conn)):
    """
    Consulta sócios com o documento informado:

    - **doc**: CNPJ se o sócio for PJ e CPF se for PF. Sem pontuação, somente digitos.
    - **cursor**: se especificado, serão exibidos apenas resultados após o cnpj_base passado ao paramêtro 'cursor'.

    exibindo de 25 em 25 resultados atualmente.
    """

    if len(doc) != 14 and len(doc) != 11:
        return get_paginacao_template([])

    if len(doc) == 11:
        doc = '***' + doc[3:9] + '**'

    parametros = {
            'doc' : doc,
            'cursor': cursor,
            }

    with conn.cursor() as cursor:
        cursor.execute(SOCIOS_QUERY, parametros)
        resultados = cursor.fetchall()
    resultados = [res[0] for res in resultados]
    return get_paginacao_template(resultados)


@app.get("/busca_difusa/")
def get_paginacao_filtros_difusos(
        razao_social: Optional[str] = None,
        cnae: Optional[str] = None,
        natureza_juridica: Optional[int] = None,
        situacao_cadastral: Optional[int] = None,
        estado: Optional[str] = None,
        municipio: Optional[str] = None,
        data_abertura_min: Optional[str] = None,
        data_abertura_max: Optional[str] = None,
        capital_social_min: Optional[float] = None,
        capital_social_max: Optional[float] = None,
        socio_doc: Optional[str] = None,
        socio_nome: Optional[str] = None,
        cursor: Optional[str] = None,
        conn=Depends(get_conn)
        ):
    """
    Consulta sócios com o documento informado:

    - **doc**: CNPJ se o sócio for PJ e CPF se for PF. Sem pontuação, somente digitos.
    - **cursor**: se especificado, serão exibidos apenas resultados após o cnpj_base passado ao paramêtro 'cursor'.

    exibindo de 25 em 25 resultados atualmente.
    """

    tem_socios_param = False
    somente_socios = not any(
        (
        razao_social,
        cnae,
        natureza_juridica,
        estado,
        municipio,
        data_abertura_min,
        data_abertura_max,
        capital_social_min,
        capital_social_max,
        situacao_cadastral
        )
    )
    if socio_nome or socio_doc:
        tem_socios_param = True

    BUSCA_DIFUSA_QUERY = get_busca_difusa_query(tem_socios_param, somente_socios)

    if razao_social:
        razao_social = normalizar_razao(razao_social)
    if socio_nome:
        socio_nome = normalizar_razao(socio_nome)
    if data_abertura_min:
        data_abertura_min = '-'.join(data_abertura_min.split('-')[::-1])
    if data_abertura_max:
        data_abertura_max = '-'.join(data_abertura_max.split('-')[::-1])

    if not (socio_doc is None or (len(socio_doc) in {11, 14})):
        socio_doc = None
    if socio_doc is not None and len(socio_doc) == 11:
        socio_doc = '***' + socio_doc[3:9] + '**'

    parametros = {
        'cnae': cnae,
        'natureza_juridica': natureza_juridica,
        'razao_social': razao_social,
        'uf': estado,
        'municipio': municipio,
        'data_abertura_min': data_abertura_min,
        'data_abertura_max': data_abertura_max,
        'capital_social_min': capital_social_min,
        'capital_social_max': capital_social_max,
        'cnpj_base': cursor,
        'situacao_cadastral': situacao_cadastral
        }

    if socio_doc is not None or socio_nome is not None:
        parametros['socio_doc'] = socio_doc
        parametros['socio_nome'] = socio_nome

    with ClientCursor(conn) as cursor:
    #with conn.cursor() as cursor:
        cursor.execute(BUSCA_DIFUSA_QUERY, parametros)
        #return cursor.mogrify(BUSCA_DIFUSA_QUERY, parametros)
        resultados = cursor.fetchall()
    resultados = [res[0] for res in resultados]
    return get_paginacao_template(resultados)


@app.get("/count/data/{data}")
def get_count_data(data: str, conn=Depends(get_conn)):
    """
    Retorna a quantidade de CNPJ's abertos em certa data.

    - **data**: data de abertura desejada no formado DD-MM-AAAA
    """
    data = '-'.join(data.split('-')[::-1])
    with conn.cursor() as cursor:
        cursor.execute(COUNT_DATA_QUERY, (data, ))
        total = cursor.fetchone()[0]
    return {'total': total}

@app.get("/count/cnpj_base/{cnpj_base}")
def get_count_raiz(cnpj_base: str, conn=Depends(get_conn)):
    """
    Consulta total de matrizes e filias a partir da base/raiz(8 primeiros caracteres) do CNPJ:
    - **cnpj_base**: 8 primeiros caracteres do número de inscrição do CNPJ.
    """
    with conn.cursor() as cursor:
        cursor.execute(COUNT_RAIZ_QUERY, (cnpj_base, ))
        total = cursor.fetchone()[0]
    return {'total': total}


@app.get("/count/razao_social/{razao_social}")
def get_paginacao_razao_social(razao_social: str, conn=Depends(get_conn)):
    """
    Consulta total de matrizes e filias a partir do nome empresarial(razão social):

    - **razao_social**: filtro por termo presente no começo(somente no começo por enquanto) da razão social
    - **cursor**: se especificado, serão exibidos apenas resultados após o cnpj_base passado ao paramêtro 'cursor'.

    exibindo de 25 em 25 resultados atualmente.
    """
    razao_social = normalizar_razao(razao_social)
    with conn.cursor() as cursor:
        cursor.execute(COUNT_RAZAO_QUERY, (razao_social, ))
        total = cursor.fetchone()[0]
    return {'total': total}


