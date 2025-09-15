import psycopg
from psycopg import sql
from dotenv import load_dotenv
import os
from config import tabelas_infos


def carregar_csv_banco(nome_tabela, csv_path, conn):
    with open(csv_path, "r", encoding="UTF-8") as f:
        with conn.cursor() as cursor:
            query = sql.SQL(
                "COPY {} FROM STDIN WITH (ENCODING 'UTF-8', DELIMITER ';', FORMAT csv, HEADER false)"
            ).format(sql.Identifier(nome_tabela))
            with cursor.copy(query) as copy:
                while data := f.read(65536):
                    copy.write(data)


def mover_entre_staging(tabela_origem, tabela_destino, conn):
    with conn.cursor() as cursor:
        query_insert = sql.SQL(
            "INSERT INTO {} SELECT * FROM {} ON CONFLICT DO NOTHING"
        ).format(sql.Identifier(tabela_destino), sql.Identifier(tabela_origem))
        query_truncate = sql.SQL(
            "TRUNCATE {}"
        ).format(sql.Identifier(tabela_origem))
        cursor.execute(query_insert)
        cursor.execute(query_truncate)


def mover_staging_producao(tabela_origem, tabela_destino, pk, colunas, conn, faz_update=True):
    colunas_update = []
    where_clausulas = []
    for col in colunas:
        coluna_query = sql.SQL("{0} = EXCLUDED.{0}").format(
                sql.Identifier(col)
                )
        where_clausula = sql.SQL("{} IS DISTINCT FROM EXCLUDED.{}").format(
                sql.Identifier(tabela_destino, col),
                sql.Identifier(col)
                )
        where_clausulas.append(where_clausula)
        colunas_update.append(coluna_query)

    query_insert = sql.SQL(
            """
            INSERT INTO {} SELECT * FROM {} ON CONFLICT ({}) DO UPDATE SET {}
            WHERE {}
            """
    ).format(
        sql.Identifier(tabela_destino),
        sql.Identifier(tabela_origem),
        sql.SQL(', ').join([sql.Identifier(col) for col in pk]),
        sql.SQL(', ').join([col for col in colunas_update]),
        sql.SQL(' OR ').join([col for col in where_clausulas])
    )
    if not faz_update:
        query_insert = sql.SQL(
            "INSERT INTO {} SELECT * FROM {} ON CONFLICT DO NOTHING"
        ).format(
            sql.Identifier(tabela_destino),
            sql.Identifier(tabela_origem)
        )
    query_truncate = sql.SQL(
        "TRUNCATE {}"
    ).format(sql.Identifier(tabela_origem))
    query_delete_socios_duplicados = "DELETE FROM socios s WHERE s.cnpj_cpf IS NULL AND (s.cnpj_base, s.nome) IN (select a.cnpj_base, a.nome from socios a join socios b on a.cnpj_base = b.cnpj_base AND a.nome = b.nome AND a.cnpj_cpf IS NULL AND b.cnpj_cpf IS NOT NULL)"
    with conn.cursor() as cursor:
        cursor.execute(query_insert)
        cursor.execute(query_truncate)
        if tabela_destino == 'socios' and faz_update:
            cursor.execute(query_delete_socios_duplicados)


def pegar_ultimo_cnpj_inserido(conn):
    with conn.cursor() as cursor:
        query_data = (
            "SELECT data_inicio_atividade FROM estabelecimentos ORDER BY data_inicio_atividade DESC LIMIT 1"
        )

        data = cursor.execute(query_data).fetchone()[0]
        query_cnpj = (
                "SELECT (cnpj_base || cnpj_ordem || cnpj_dv) AS cnpj " +
                "FROM estabelecimentos where data_inicio_atividade = (%s)::date " +
                "ORDER BY (cnpj_base, cnpj_ordem, cnpj_dv) DESC LIMIT 1"
                )
        cnpj = cursor.execute(query_cnpj, (data, )).fetchone()[0]
    return cnpj


