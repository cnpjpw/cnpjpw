from fastapi import Depends, FastAPI, Response, status, Query, Body, HTTPException
from typing import Optional
import psycopg
from psycopg import ClientCursor
import os
from math import ceil
from time import time
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv
from queries import (
        CNPJ_QUERY,
        RAZAO_QUERY,
        RAIZ_QUERY,
        DATA_ABERTURA_QUERY,
        MUNICIPIOS_QUERY,
        CNAES_QUERY,
        NATUREZAS_QUERY,
        COUNT_DATA_QUERY,
        COUNT_RAIZ_QUERY,
        COUNT_RAZAO_QUERY,
        SOCIOS_QUERY,
        get_busca_difusa_query
        )
import unicodedata
from datetime import datetime
from pydantic import BaseModel
from typing import List

load_dotenv()
app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
bd_nome = os.getenv('BD_NOME')
bd_usuario = os.getenv('BD_USUARIO')


class Count(BaseModel):
    total: int

class CnaesFiscaisSecundario(BaseModel):
    codigo: int
    descricao: str


class Socio(BaseModel):
    identificador_entidade: int
    nome: str
    cnpj_cpf: str
    qualificacao_descricao: str
    qualificacao_codigo: int
    data_entrada_sociedade: str
    pais: str | None = None
    cpf_representante: str | None = None
    nome_representante: str | None = None
    qualificacao_representante_codigo: int
    qualificacao_representante_descricao: str
    faixa_etaria_codigo: int
    faixa_etaria_descricao: str
    identificador_entidade_descricao: str


class Estabelecimento(BaseModel):
    cnpj_base: str
    nome_empresarial: str | None = None
    natureza_juridica: int
    qualificacao_responsavel: int
    capital_social: float
    porte_empresa: int
    ente_federativo: str | None = None
    cnpj_ordem: str
    cnpj_dv: str
    identificador: int
    nome_fantasia: str | None = None
    situacao_cadastral: int
    data_situacao_cadastral: str
    motivo_situacao_cadastral: int
    nome_cidade_exterior: str | None = None
    pais: str | None = None
    data_inicio_atividade: str
    cnae_fiscal_principal: int
    cnaes_fiscais_secundarios: List[CnaesFiscaisSecundario]
    tipo_logradouro: str
    logradouro: str
    numero: str
    complemento: str
    bairro: str
    cep: str
    uf: str
    municipio: int
    ddd1: str | None = None
    telefone_1: str | None = None
    ddd2: str | None = None
    telefone_2: str | None = None
    fax: str | None = None
    ddd_fax: str | None = None
    correio_eletronico: str | None = None
    situacao_especial: str | None = None
    data_situacao_especial: str | None = None
    opcao_simples: bool
    data_opcao_simples: str
    data_exclusao_simples: str
    opcao_mei: bool
    data_opcao_mei: str
    data_exclusao_mei: str
    natureza_juridica_desc: str
    motivo_situacao_desc: str
    municipio_desc: str
    pais_desc: str | None = None
    cnae_fiscal_principal_descricao: str
    identificador_descricao: str
    porte_empresa_descricao: str
    situacao_cadastral_descricao: str
    socios: List[Socio]


class ItemAuxiliar(BaseModel):
    descricao: str
    codigo: int


class Auxiliares(BaseModel):
    resultados: List[ItemAuxiliar]


class EstabelecimentoPaginacaoItem(BaseModel):
    cnpj_base: str
    cnpj_ordem: str
    cnpj_dv: str
    cnpj: str
    nome_empresarial: str | None = None


class EmpresaPaginacaoItem(BaseModel):
    cnpj_base: str
    nome_empresarial: str | None = None


class SocioPaginacaoItem(BaseModel):
    cnpj_base: str
    nome: str
    cnpj_cpf: str


class PaginacaoEstabelecimentos(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[EstabelecimentoPaginacaoItem]

class PaginacaoEmpresas(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[EmpresaPaginacaoItem]


class PaginacaoSocios(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[SocioPaginacaoItem]


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


@app.get("/cnpj/{cnpj}",
         response_description="Página contendo informações do estabelecimento correspondente ao CNPJ passado",
         summary="Retorna estabelecimento com CNPJ indicado",
         response_model=Estabelecimento,
         status_code=200
         )
def get_cnpj(cnpj: str, response: Response, conn=Depends(get_conn)):
    cnpj_base = cnpj[:8]
    cnpj_ordem = cnpj[8:12]
    cnpj_dv = cnpj[12:]
    with conn.cursor() as cursor:
        cursor.execute(CNPJ_QUERY, (cnpj_base, cnpj_ordem, cnpj_dv))
        res_json = cursor.fetchone()
    if not res_json:
        raise HTTPException(status_code=404, detail="CNPJ não encontrado")

    return res_json[0]


@app.get("/razao_social/{razao_social}",
         response_description="Página de resultados contendo lista de empresas abertas contendo a razão social indicada",
         summary="Retorna página com lista de empresas contendo a razão indicada",
         response_model=PaginacaoEmpresas,
         status_code=200
         )
def get_paginacao_razao_social(razao_social: str, cursor: Optional[str] = None, conn=Depends(get_conn)):
    """
    Consulta estabelecimentos a partir do nome empresarial(razão social):

    - **razao_social**: filtro por razão social(atualmente fazendo o match pelo começo da string).
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


@app.get("/cnpj_base/{cnpj_base}",
         response_description="Página de resultados contendo lista de estabelecimentos abertos de certa empresa referente ao cnpj_base passado",
         summary="Retorna página com lista de estabelecimentos da empresa correspondente ao cnpj básico passado",
         response_model=PaginacaoEstabelecimentos,
         status_code=200
         )
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


@app.get("/data/{data}",
         response_description="página de resultados contendo lista de estabelecimentos abertos na data passada",
         summary="Retorna página com lista de estabelecimentos abertos na data",
         response_model=PaginacaoEstabelecimentos,
         status_code=200
         )
def get_paginacao_data(data: str, cursor: Optional[str] = None, conn=Depends(get_conn)):
    """
    Consulta CNPJ's abertos em uma certa data:

    - **data**: data de abertura desejada no formado DD-MM-AAAA
    - **cursor**: se especificado, serão exibidos apenas resultados após o cnpj passado ao paramêtro 'cursor'.

    exibindo de 25 em 25 resultados atualmente.
    """
    try:
        datetime.strptime(data, '%d-%m-%Y')
    except ValueError:
        raise HTTPException(status_code=418, detail="valor de data inválido.")
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


@app.get("/socio/{doc}",
         response_description="página de resultados contendo lista dos socios com documento(CPF/CNPJ) passado",
         summary="Retorna página com lista de socios que batem com o documento passado",
         response_model=PaginacaoSocios,
         status_code=200
         )
def get_paginacao_socio(doc: str, cursor: Optional[str] = None, conn=Depends(get_conn)):
    """
    Consulta sócios com o documento informado:

    - **doc**: CNPJ se o sócio for PJ e CPF se for PF. Sem pontuação, somente digitos.\n
        OBS: Como só temos acesso ao CPF mascarado(não acontece com o CNPJ) no formato "\*\*\*000000\*\*" na base de dados, só podemos então
        levar em consideração os dígitos centrais para o match, então uma busca por 00000000000, por exemplo, na verdade considera 1000 possibilidades:\n
                                                        [000-999]000000-xx\n
        Mas, de toda forma, se o sócio estiver no banco com o CPF indicado aparecerá na paginação(junto com alguns falso-positivos).
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


@app.get("/busca_difusa/",
         response_description="página de resultados contendo lista dos estabelecimentos filtrados",
         summary="Retorna página com lista de estabelecimentos que batem com os filtros",
         response_model=PaginacaoEstabelecimentos,
         status_code=200
         )
def get_paginacao_filtros_difusos(
        razao_social: Optional[str] = None,
        cnae: Optional[int] = None,
        natureza_juridica: Optional[int] = None,
        situacao_cadastral: Optional[int] = None,
        uf: Optional[str] = None,
        municipio: Optional[int] = None,
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
    Consulta por filtros diversos:

    - **razao_social**: filtro por razão social(atualmente fazendo o match pelo começo da string).
    - **cnae**: filtro por cnae - Sem pontuação, somente os digitos.
    - **natureza_jurídica**: filtro por natureza jurídica - Sem pontuação, somente os digitos.
    - **municipio**: filtro por município - Somente o código númerico correspondente ao município(encontra-se em /municipios).
    - **situacao cadastral**: filtro da situacao cadastral. Passe o código numérico correspondente a situacao
    - **uf**: filtro por unidade federativa. Passe a sigla da UF.
    - **data_abertura_min**: filtro por data de abertura. Passe a data de abertura mínima no formato DD-MM-AAAA.
    - **data_abertura_max**: filtro por data de abertura. Passe a data de abertura máxima no formato DD-MM-AAAA.
    - **capital_social_min**: filtro por capital social. Passe o capital social mínimo como float(com ponto separando a parte decimal, como 1000.50).
    - **capital_social_min**: filtro por capital social. Passe o capital social máximo como float(com ponto separando a parte decimal, como 1000.50).
    - **socio_doc**: CNPJ se o sócio for PJ e CPF se for PF. Sem pontuação, somente digitos.
    - **socio_nome**: Consulta por nome do sócio(atualmente fazendo o match pelo começo da string)
    - **cursor**: se especificado, serão exibidos apenas resultados após o cnpj passado ao paramêtro 'cursor'.

    exibindo de 250 em 250 resultados atualmente.
    """

    #TODO: mensagens de erro personalizadas
    if (capital_social_min is not None
        and capital_social_max is not None
        and capital_social_min > capital_social_max):
        return get_paginacao_template([], limite=250)

    if cursor is not None and len(cursor) != 14:
        return get_paginacao_template([], limite=250)

    tem_socios_param = False
    somente_socios = not any(
        (
        razao_social,
        cnae,
        natureza_juridica,
        uf,
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
        try:
            datetime.strptime(data_abertura_min, '%d-%m-%Y')
        except ValueError:
            raise HTTPException(status_code=418, detail="valor de data inválido.")
        data_abertura_min = '-'.join(data_abertura_min.split('-')[::-1])
    if data_abertura_max:
        try:
            datetime.strptime(data_abertura_max, '%d-%m-%Y')
        except ValueError:
            raise HTTPException(status_code=418, detail="valor de data inválido.")
        data_abertura_max = '-'.join(data_abertura_max.split('-')[::-1])
    if (data_abertura_min and data_abertura_max and
        datetime.strptime(data_abertura_max, '%Y-%m-%d')  < datetime.strptime(data_abertura_min, '%Y-%m-%d')
        ):
            return get_paginacao_template([], limite=250)

    if not (socio_doc is None or (len(socio_doc) in {11, 14})):
        socio_doc = None
    if socio_doc is not None and len(socio_doc) == 11:
        socio_doc = '***' + socio_doc[3:9] + '**'

    cnpj_base = None
    cnpj_ordem = None
    cnpj_dv = None

    if cursor is not None and len(cursor) == 14:
        cnpj_base = cursor[:8]
        cnpj_ordem = cursor[8:12]
        cnpj_dv = cursor[12:]

    parametros = {
        'cnae': cnae,
        'natureza_juridica': natureza_juridica,
        'razao_social': razao_social,
        'uf': uf,
        'municipio': municipio,
        'data_abertura_min': data_abertura_min,
        'data_abertura_max': data_abertura_max,
        'capital_social_min': capital_social_min,
        'capital_social_max': capital_social_max,
        'cnpj_base': cnpj_base,
        'cnpj_ordem': cnpj_ordem,
        'cnpj_dv': cnpj_dv,
        'situacao_cadastral': situacao_cadastral,
        'socio_doc': socio_doc,
        'socio_nome': socio_nome
        }
    with ClientCursor(conn) as cursor:
    #with conn.cursor() as cursor:
        cursor.execute(BUSCA_DIFUSA_QUERY, parametros)
        #return cursor.mogrify(BUSCA_DIFUSA_QUERY, parametros)
        resultados = cursor.fetchall()
    resultados = [res[0] for res in resultados]
    return get_paginacao_template(resultados, limite=250)


@app.get("/municipios/",
         response_description="descrição(nome) e código de todos municípios",
         summary="Retorna todos municípios",
         response_model=Auxiliares,
         status_code=200
         )
def get_municipios(conn=Depends(get_conn)):
    """
    Retorna o nome e o código de todos os municípios presentes no banco
    """
    with conn.cursor() as cursor:
        cursor.execute(MUNICIPIOS_QUERY)
        resultados = cursor.fetchall()
    resultados = [res[0] for res in resultados]
    return {'resultados': resultados}


@app.get("/cnaes/",
         response_description="descrição e código de todos cnaes",
         summary="Retorna todos cnaes",
         response_model=Auxiliares,
         status_code=200
         )
def get_cnaes(conn=Depends(get_conn)):
    """
    Retorna a descrição e o código de todos os cnaes presentes no banco
    """
    with conn.cursor() as cursor:
        cursor.execute(CNAES_QUERY)
        resultados = cursor.fetchall()
    resultados = [res[0] for res in resultados]
    return {'resultados': resultados}


@app.get("/naturezas/",
         response_description="descrição e código de todas naturezas jurídicas",
         summary="Retorna todas naturezas jurídicas",
         response_model=Auxiliares,
         status_code=200
         )
def get_cnaes(conn=Depends(get_conn)):
    """
    Retorna a descrição e o código de todos as naturezas juridicas presentes no banco
    """
    with conn.cursor() as cursor:
        cursor.execute(NATUREZAS_QUERY)
        resultados = cursor.fetchall()
    resultados = [res[0] for res in resultados]
    return {'resultados': resultados}


@app.get("/count/data/{data}",
         response_description="Quantidade de estabelecimentos abertos em certa data",
         summary="Retorna quantidade de empresas abertas na data",
         response_model=Count,
         status_code=200
         )
def get_count_data(data: str, conn=Depends(get_conn)):
    """
    Retorna a quantidade de CNPJ's abertos em certa data:
    - **data**: data de abertura desejada no formado DD-MM-AAAA
    """
    data = '-'.join(data.split('-')[::-1])
    with conn.cursor() as cursor:
        cursor.execute(COUNT_DATA_QUERY, (data, ))
        total = cursor.fetchone()[0]
    return {'total': total}


@app.get("/count/cnpj_base/{cnpj_base}",
         response_description="Quantidade de estabelecimentos de certa empresa",
         summary="Retorna quantidade de estabelecimentos da empresa",
         response_model=Count,
         status_code=200
         )
def get_count_raiz(cnpj_base: str, conn=Depends(get_conn)):
    """
    Consulta total de matrizes e filias a partir da base/raiz(8 primeiros caracteres) do CNPJ:
    - **cnpj_base**: 8 primeiros caracteres do número de inscrição do CNPJ.
    """
    with conn.cursor() as cursor:
        cursor.execute(COUNT_RAIZ_QUERY, (cnpj_base, ))
        total = cursor.fetchone()[0]
    return {'total': total}


@app.get("/count/razao_social/{razao_social}",
         response_description="Quantidade de empresas com certa razão social",
         summary="Retorna quantidade de empresas contendo a razão social indicada",
         response_model=Count,
         status_code=200
         )
def get_count_razao(razao_social: str, conn=Depends(get_conn)):
    """
    Consulta total de matrizes e filias a partir do nome empresarial(razão social):
    - **razao_social**: filtro por termo presente no começo(somente no começo por enquanto) da razão social
    """
    razao_social = normalizar_razao(razao_social)
    with conn.cursor() as cursor:
        cursor.execute(COUNT_RAZAO_QUERY, (razao_social, ))
        total = cursor.fetchone()[0]
    return {'total': total}


