import json
import psycopg
from cnpjpw_scrapers import gerar_novos_cnpjs


def pegar_ultimo_cnpj_inserido(conn):
    with conn.cursor() as cursor:
        query_data = (
            "SELECT data_inicio_atividade FROM estabelecimentos ORDER BY data_inicio_atividade DESC LIMIT 1"
        )
        data = cursor.execute(query_data).fetchone()[0]
        query_cnpj = (
                "SELECT cnpj_base AS cnpj " +
                "FROM estabelecimentos where data_inicio_atividade = (%s)::date " +
                "ORDER BY cnpj_base DESC LIMIT 1"
                )
        cnpj = cursor.execute(query_cnpj, (data, )).fetchone()[0]
    return cnpj


def pegar_primeiro_cnpj_dia(conn):
    with conn.cursor() as cursor:
        query_data = (
            "SELECT data_inicio_atividade - INTERVAL '1 day' FROM estabelecimentos ORDER BY data_inicio_atividade DESC LIMIT 1"
        )
        data = cursor.execute(query_data).fetchone()[0]
        query_primeiro = (
            "SELECT cnpj_base FROM estabelecimentos WHERE data_inicio_atividade = (%s)::date ORDER BY cnpj_base ASC LIMIT 1"
        )
        primeiro = cursor.execute(query_primeiro, (data, )).fetchone()[0]
    return primeiro


def pegar_buracos_dia(conn):
    with conn.cursor() as cursor:
        primeiro = pegar_primeiro_cnpj_dia(conn)
        ultimo = pegar_ultimo_cnpj_inserido(conn)
        query_cnpjs = (
            "select cnpj_base || cnpj_ordem || cnpj_dv from estabelecimentos where cnpj_base > (%s)::bpchar AND cnpj_base < (%s)::bpchar ORDER BY cnpj_base"
        )
        res = cursor.execute(query_cnpjs, (primeiro, ultimo)).fetchall()
    cnpjs = [k[0] for k in res]
    total = int(cnpjs[-1][:8]) - int(cnpjs[0][:8]) + 1
    gerados = gerar_novos_cnpjs(cnpjs[0], total)
    dif_cnpjs = list(set(gerados) - set(cnpjs))
    return dif_cnpjs


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


