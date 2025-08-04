import psycopg
from psycopg import sql
from dotenv import load_dotenv
import os
from config import tabelas_infos


def carregar_csv_banco(nome_tabela, csv_path, conn):
    with open(csv_path, "r", encoding="latin-1") as f:
        with conn.cursor() as cursor:
            query = sql.SQL(
                "COPY {} FROM STDIN WITH (ENCODING 'utf-8', DELIMITER ';', FORMAT csv, HEADER false)"
            ).format(sql.Identifier(nome_tabela))
            with cursor.copy(query) as copy:
                while data := f.read(65536):
                    copy.write(data)
            conn.commit()


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
        conn.commit()


def mover_staging_producao(tabela_origem, tabela_destino, pk, colunas, conn, faz_update=True):
    #TEM Q REFATORAR
    colunas_update = []
    for col in colunas:
        coluna_query = sql.SQL("{0} = EXCLUDED.{0}").format(
                sql.Identifier(col)
                )
        colunas_update.append(coluna_query)
    with conn.cursor() as cursor:
        query_insert = sql.SQL(
            "INSERT INTO {} SELECT * FROM {} ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
                sql.Identifier(tabela_destino), 
                sql.Identifier(tabela_origem),
                sql.SQL(', ').join([sql.Identifier(col) for col in pk]),
                sql.SQL(', ').join([col for col in colunas_update])
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
        cursor.execute(query_insert)
        cursor.execute(query_truncate)
        conn.commit()




if __name__ == '__main__':
    load_dotenv()
    bd_nome = os.getenv('BD_NOME')
    bd_usuario = os.getenv('BD_USUARIO')

    with psycopg.connect(dbname=bd_nome, user=bd_usuario) as conn:
        mover_staging_producao(
                'socios_staging2',
                'socios',
                tabelas_infos['socios']['pk'],
                tabelas_infos['socios']['colunas'],
                conn
                )


