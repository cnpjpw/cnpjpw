import psycopg
from psycopg import sql
from dotenv import load_dotenv
import os

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


if __name__ == '__main__':
    load_dotenv()
    bd_nome = os.getenv('BD_NOME')
    bd_usuario = os.getenv('BD_USUARIO')

    with psycopg.connect(dbname=bd_nome, user=bd_usuario) as conn:
        csv_tabelas = {'dados_simples_temp': './Simples.csv'}
        for nome_tabela, csv_path in csv_tabelas.items():
            carregar_csv_banco(nome_tabela, csv_path, conn)


