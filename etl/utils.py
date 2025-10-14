import json
import psycopg
from cnpjpw_scrapers import gerar_novos_cnpjs


def gerar_nova_data(mes, ano, passo):
    q, r = divmod((mes - 1) + passo, 12)
    return (r + 1, ano + q)

def pegar_primeiro_blocado(cnpjs, quant_adjacencias_min=120, max_dist_adjacentes=3):
    if len(cnpjs) <= quant_adjacencias_min:
        return cnpjs[-1]

    acc_adjacencias = 0
    for i in range(1, len(cnpjs)):
        distancia = abs(int(cnpjs[i]) - int(cnpjs[i - 1]))
        if distancia > max_dist_adjacentes:
            acc_adjacencias = 0
            continue
        acc_adjacencias += 1
        if acc_adjacencias == quant_adjacencias_min:
            return cnpjs[i - acc_adjacencias]
    return cnpjs[-1]


def pegar_ultimo_cnpj_inserido(conn):
    with conn.cursor() as cursor:
        query_data = (
            "SELECT data_inicio_atividade FROM estabelecimentos " +
            "ORDER BY data_inicio_atividade DESC LIMIT 1"
        )
        data = cursor.execute(query_data).fetchone()[0]
        query_cnpj = (
                "SELECT cnpj_base FROM estabelecimentos " +
                "WHERE data_inicio_atividade = (%s)::date " +
                "AND cnpj_ordem='0001' " +
                "ORDER BY cnpj_base DESC"
                )
        cnpjs = cursor.execute(query_cnpj, (data, )).fetchall()
        cnpjs = [cnpj[0] for cnpj in cnpjs]

    ultimo = pegar_primeiro_blocado(cnpjs)
    return ultimo


def pegar_primeiro_cnpj_dia(conn):
    with conn.cursor() as cursor:
        query_data = (
            "SELECT data_inicio_atividade - INTERVAL '1 day' " +
            "FROM estabelecimentos " +
            "ORDER BY data_inicio_atividade DESC LIMIT 1"
        )
        data = cursor.execute(query_data).fetchone()[0]
        query_primeiro = (
            "SELECT cnpj_base FROM estabelecimentos " +
            "WHERE data_inicio_atividade = (%s)::date " +
            "AND cnpj_ordem='0001' " +
            "ORDER BY cnpj_base ASC"
        )
        cnpjs = cursor.execute(query_primeiro, (data, )).fetchall()
        cnpjs = [cnpj[0] for cnpj in cnpjs]

    primeiro = pegar_primeiro_blocado(cnpjs)
    return primeiro


def pegar_vagos_dia(conn):
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
    percentual = (len(dif_cnpjs) * 100) / total
    return (dif_cnpjs, round(percentual, 2))


def ler_data_json(path):
    with open(path, "r") as f:
        data_pasta = json.load(f)

    return (data_pasta['mes'], data_pasta['ano'])


def acrescentar_mes_json(path, mes, ano):
    mes, ano = gerar_nova_data(mes, ano, 1)
    with open(path, "w") as f:
        data_dic = {
            'mes': mes,
            'ano': ano
        }
        json.dump(data_dic, f)


def pegar_matrizes_banco(primeiro, ultimo, conn):
    with conn.cursor() as cursor:
        query_cnpjs = (
            "SELECT cnpj_base || cnpj_ordem || cnpj_dv FROM estabelecimentos WHERE " +
            "cnpj_base >= (%s)::bpchar AND cnpj_base <= (%s)::bpchar AND cnpj_ordem = '0001' ORDER BY cnpj_base"
            )
        res = cursor.execute(query_cnpjs, (primeiro, ultimo)).fetchall()
    cnpjs = [r[0] for r in res]
    return cnpjs


