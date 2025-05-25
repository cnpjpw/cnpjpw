from fastapi import Depends, FastAPI, Response, status, Query, Body
from typing import Optional
import psycopg
import os
from math import ceil
from time import time
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
bd_nome = os.getenv('BD_NOME')
bd_usuario = os.getenv('BD_USUARIO')

def get_conn():
    return psycopg.connect(dbname=bd_nome, user=bd_usuario)

@app.get("/cnpj/{cnpj}", status_code=200)
def get_cnpj(cnpj: str, response: Response, conn=Depends(get_conn)):
    cnpj_base = cnpj[:8]
    cnpj_ordem = cnpj[8:12]
    cnpj_dv = cnpj[12:]
    try:
        with conn.cursor() as cursor:
            cursor.execute(
            """ 
            SELECT row_to_json(result)
            FROM (
                SELECT
                    e.*,
                    est.*,
                    ds.opcao_simples,
                    ds.data_opcao_simples,
                    ds.data_exclusao_simples,
                    ds.opcao_mei,
                    ds.data_opcao_mei,
                    ds.data_exclusao_mei,
                    nj.descricao AS natureza_juridica_desc,
                    ms.descricao AS motivo_situacao_desc,
                    m.descricao AS municipio_desc,
                    p.descricao AS pais_desc,
                    c.descricao AS cnae_fiscal_principal_descricao,
                    imf.descricao AS identificador_descricao,
                    pe.descricao AS porte_empresa_descricao,
                    sc.descricao AS situacao_cadastral_descricao,
                    (
                        SELECT json_agg(json_build_object(
                            'id', s.socios_id,
                            'identificador_entidade', s.identificador,
                            'nome', s.nome,
                            'cnpj_cpf', s.cnpj_cpf,
                            'qualificacao_descricao', qs.descricao,
                            'qualificacao_codigo', s.qualificacao,
                            'data_entrada_sociedade', s.data_entrada_sociedade,
                            'pais', ps.descricao,
                            'cpf_representante', s.cpf_representante,
                            'nome_representante', s.nome_representante,
                            'qualificacao_representante_codigo', s.qualificacao_representante,
                            'qualificacao_representante_descricao', qr.descricao,
                            'faixa_etaria_codigo', s.faixa_etaria,
                            'faixa_etaria_descricao', fe.descricao,
                            'identificador_entidade_descricao', id_s.descricao
                        ))
                        FROM socios s
                        LEFT JOIN qualificacoes_socios qs ON s.qualificacao = qs.codigo
                        LEFT JOIN paises ps ON s.pais = ps.codigo
                        LEFT JOIN identificadores_socios id_s ON s.identificador = id_s.codigo
                        LEFT JOIN faixas_etarias fe ON fe.codigo = s.faixa_etaria
                        LEFT JOIN qualificacoes_representantes qr ON qr.codigo = s.qualificacao_representante
                        WHERE s.cnpj_base = est.cnpj_base
                    ) AS socios,
                    (
                        SELECT json_agg(json_build_object(
                            'codigo', cs.codigo,
                            'descricao', c.descricao
                        ))
                        FROM unnest(est.cnaes_fiscais_secundarios) AS cs(codigo)
                        LEFT JOIN cnaes c ON c.codigo = cs.codigo
                    ) AS cnaes_fiscais_secundarios

                FROM estabelecimentos est
                JOIN empresas e ON est.cnpj_base = e.cnpj_base
                LEFT JOIN dados_simples ds ON est.cnpj_base = ds.cnpj_base
                LEFT JOIN naturezas_juridicas nj ON e.natureza_juridica = nj.codigo
                LEFT JOIN cnaes c ON est.cnae_fiscal_principal = c.codigo
                LEFT JOIN motivos_situacoes ms ON est.motivo_situacao_cadastral = ms.codigo
                LEFT JOIN municipios m ON est.municipio = m.codigo
                LEFT JOIN paises p ON est.pais = p.codigo
                LEFT JOIN identificador_matriz_filial imf ON est.identificador = imf.codigo
                LEFT JOIN portes_empresas pe ON pe.codigo = e.porte_empresa
                LEFT JOIN situacoes_cadastrais sc ON sc.codigo = est.situacao_cadastral

                WHERE est.cnpj_base=(%s)::bpchar
                  AND est.cnpj_ordem=(%s)::bpchar
                  AND est.cnpj_dv=(%s)::bpchar
                ) result;

            """
            , (cnpj_base, cnpj_ordem, cnpj_dv)
            )
            res_json = cursor.fetchone()
            conn.commit()
    except BaseException:
        conn.rollback()
    finally:
        conn.close()
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
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                     """
                        SELECT row_to_json(result) FROM (
                            SELECT est.cnpj_base,
                                est.cnpj_ordem,
                                est.cnpj_dv,
                                est.nome_fantasia,
                                est.data_inicio_atividade,
                                sc.descricao AS situacao_cadastral,
                                e.nome_empresarial
                            FROM estabelecimentos est
                            LEFT JOIN empresas e ON est.cnpj_base = e.cnpj_base
                            LEFT JOIN situacoes_cadastrais sc ON est.situacao_cadastral = sc.codigo
                            WHERE data_inicio_atividade = (%s)::date
                            ORDER BY est.cnpj_base, est.cnpj_ordem, est.cnpj_dv ASC LIMIT 25 OFFSET (%s)
                        ) result;
                         """,
                     (data, offset)
                     )
            results = cursor.fetchall()
            cursor.execute(
                     "SELECT COUNT(*) FROM estabelecimentos WHERE (data_inicio_atividade = (%s))", 
                     (data, )
                     )
            total = cursor.fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
    finally:
        conn.close()
    quant_paginacoes = ceil(total[0] / 25)
    return {
            'quantidade_total_resultados':  total[0],
            'limite_resultados_paginacao': 25,
            'paginacao_atual': p,
            'quantidade_paginacoes': quant_paginacoes,
            'resultados_paginacao': [res[0] for res in results],
    }



@app.get("/cnpj_base/{cnpj_base}")
def get_paginacao_data(cnpj_base: str, p: int = 1, conn=Depends(get_conn)):
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
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                     """
                        SELECT row_to_json(result) FROM (
                            SELECT est.cnpj_base,
                                est.cnpj_ordem,
                                est.cnpj_dv,
                                est.nome_fantasia,
                                est.data_inicio_atividade,
                                sc.descricao AS situacao_cadastral,
                                e.nome_empresarial
                            FROM estabelecimentos est
                            LEFT JOIN empresas e ON est.cnpj_base = e.cnpj_base
                            LEFT JOIN situacoes_cadastrais sc ON est.situacao_cadastral = sc.codigo
                            WHERE est.cnpj_base = (%s)
                            ORDER BY (est.cnpj_base, est.cnpj_ordem, est.cnpj_dv) ASC LIMIT 25 OFFSET (%s)
                        ) result;
                         """,
                     (cnpj_base, offset)
                     )
            results = cursor.fetchall()
            cursor.execute(
                    "SELECT COUNT(*) FROM estabelecimentos WHERE (cnpj_base = (%s)::bpchar)", 
                     (cnpj_base, )
                     )
            total = cursor.fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
    finally:
        conn.close()
    quant_paginacoes = ceil(total[0] / 25)
    return {
            'quantidade_total_resultados':  total[0],
            'limite_resultados_paginacao': 25,
            'paginacao_atual': p,
            'quantidade_paginacoes': quant_paginacoes,
            'resultados_paginacao': [res[0] for res in results],
    }





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
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                     """
                     SELECT row_to_json(result) FROM (
                        SELECT 
                            est.cnpj_base,
                            est.cnpj_ordem,
                            est.cnpj_dv
                            -- est.nome_fantasia,
                            -- est.data_inicio_atividade,
                            -- sc.descricao AS situacao_cadastral,
                            -- e.nome_empresarial
                        FROM estabelecimentos est
                        LEFT JOIN empresas e ON est.cnpj_base = e.cnpj_base
                        -- LEFT JOIN situacoes_cadastrais sc ON est.situacao_cadastral = sc.codigo
                        WHERE (
                            ( (%(data_abertura_min)s::date IS NULL) OR (est.data_inicio_atividade >= (%(data_abertura_min)s)::date) ) AND
                            ( (%(data_abertura_max)s::date IS NULL) OR (est.data_inicio_atividade <= (%(data_abertura_max)s)::date) ) AND
                            ( (%(razao_social)s::text IS NULL) OR (e.nome_empresarial LIKE UPPER(%(razao_social)s)) ) AND
                            ( (%(cnpj_base)s::bpchar IS NULL) OR (est.cnpj_base = (%(cnpj_base)s)::bpchar) ) AND
                            ( (%(situacao_cadastral)s::integer IS NULL) OR (est.situacao_cadastral = (%(situacao_cadastral)s)) ) AND
                            ( (%(capital_social_min)s::numeric IS NULL) OR (e.capital_social >= (%(capital_social_min)s)) ) AND
                            ( (%(capital_social_max)s::numeric IS NULL) OR (e.capital_social <= (%(capital_social_max)s)) ) AND
                            ( (%(uf)s::bpchar IS NULL) OR (est.uf = (%(uf)s)) ) AND
                            ( (%(municipio)s::integer IS NULL) OR (est.municipio = (%(municipio)s)) ) AND
                            ( (%(cnae_principal)s::bpchar IS NULL) OR (est.cnae_fiscal_principal = (%(cnae_principal)s::bpchar)) ) AND
                            ( (%(natureza_juridica)s::integer IS NULL) OR (e.natureza_juridica = (%(natureza_juridica)s)) )
                        )
                        ORDER BY est.cnpj_base, est.cnpj_ordem, est.cnpj_dv LIMIT 25 OFFSET (%(offset)s)
                    ) result;

                    """,
                    parametros_filtro
                    )
            results = cursor.fetchall()
            """
            cursor.execute(
                     "SELECT COUNT(*) FROM estabelecimentos WHERE (data_inicio_atividade = (%s))", 
                     (data, )
                     )
            total = cursor.fetchone()
            """
        conn.commit()
    except BaseException as e:
        conn.rollback()
        conn.close()
        raise e
    #quant_paginacoes = ceil(total[0] / 25)
    return {
            #'quantidade_total_resultados':  total[0],
            'limite_resultados_paginacao': 25,
            'paginacao_atual': p,
            #'quantidade_paginacoes': quant_paginacoes,
            'resultados_paginacao': [res[0] for res in results],
    }
