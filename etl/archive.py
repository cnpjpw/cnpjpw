import shutil
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import zipfile
from config import PRINCIPAIS


def extrair_arquivo_zip(path_zip, destino):
    with zipfile.ZipFile(path_zip, 'r') as archive:
        arqs_internos = archive.namelist()
        for nome_interno in arqs_internos:
            archive.extract(nome_interno, path=destino)
    return nome_interno


def recriar_acumuladores_archive(path_archive):
    data_inicial = datetime(2026, 2, 11, tzinfo=timezone(timedelta(hours=-3)))
    path_horas = path_archive / 'horas_passadas'
    nomes_zips = [z.parts[-1] for z in path_horas.iterdir()]
    for nome_zip in nomes_zips:
        data_zip = datetime.fromisoformat(nome_zip.split('.zip')[0])
        pasta_dia, pasta_semana = encontrar_pastas_archive(data_zip, data_inicial)
        dir_tmp = path_archive / 'tmp'
        extrair_arquivo_zip(path_horas / nome_zip, dir_tmp)
        archive_dias = path_archive / 'dias_passados' 
        archive_semanas = path_archive / 'semanas_passadas'
        path_dia_atual = archive_dias / pasta_dia
        path_semana_atual = archive_semanas / pasta_semana
        for p in [path_dia_atual, path_semana_atual]:
            p.mkdir(parents=True, exist_ok=True)
            for csv_nome in PRINCIPAIS:
                (p / (csv_nome + '.csv')).touch() 
        for file in dir_tmp.iterdir():
            nome = file.parts[-1]
            if nome not in [n + '.csv' for n in PRINCIPAIS]:
                continue
            acumular_csv(file.parts[-1], dir_tmp, path_dia_atual)
            acumular_csv(file.parts[-1], dir_tmp, path_semana_atual)
            file.unlink()


def acumular_csv(nome, path_entrada, path_saida):
    destino = path_saida / nome
    with open(destino, 'ab') as wfd:
        arquivo_particionado = path_entrada / nome
        with open(arquivo_particionado, 'rb') as fd:
            shutil.copyfileobj(fd, wfd)


def encontrar_pastas_archive(data_atual, data_inicial):
    QUANT_DIAS_SEMANA = 7
    dias = (data_atual - data_inicial).days
    resto = dias % QUANT_DIAS_SEMANA
    data_inicial_semanal = data_inicial + timedelta(days=(dias - resto))
    data_final_semanal = data_inicial_semanal + timedelta(days=QUANT_DIAS_SEMANA - 1)

    data_atual_str = data_atual.strftime("%d-%m-%Y")
    data_inicial_semana_str = data_inicial_semanal.strftime("%d-%m-%Y")
    data_final_semana_str = data_final_semanal.strftime("%d-%m-%Y")

    pasta_archive_semana =  data_inicial_semana_str + ':' + data_final_semana_str
    pasta_archive_dia = data_atual_str

    return (pasta_archive_dia, pasta_archive_semana)
    

def compactar_archives_passados(path_periodo, pasta_atual: str):
    for file in path_periodo.iterdir():
        if not file.is_dir() or file == (path_periodo / pasta_atual):
            continue
        with zipfile.ZipFile(path_periodo / (file.parts[-1] + ".zip"), "w", compression=zipfile.ZIP_DEFLATED) as z:
            for arquivo in file.rglob("*"):
                if arquivo.is_file():
                    z.write(arquivo, arquivo.relative_to(file))
        shutil.rmtree(file)


def arquivar_csvs(path_origem, path_destino):
    data_inicial = datetime(2026, 2, 11, tzinfo=timezone(timedelta(hours=-3)))
    data_atual = datetime.now(tz=timezone(timedelta(hours=-3)))
    pasta_dia, pasta_semana = encontrar_pastas_archive(data_atual, data_inicial)
    path_dias_passados = path_destino / 'dias_passados'
    path_dia = path_dias_passados / pasta_dia
    path_semanas_passadas = path_destino / 'semanas_passadas'
    path_semana = path_semanas_passadas / pasta_semana
    path_dia.mkdir(parents=True, exist_ok=True)
    path_semana.mkdir(parents=True, exist_ok=True)
    compactar_archives_passados(path_dias_passados, pasta_dia)
    compactar_archives_passados(path_semanas_passadas, pasta_semana)
    for arquivo in path_origem.rglob("*"):
        acumular_csv(arquivo.relative_to(path_origem), path_origem, path_dia)
        acumular_csv(arquivo.relative_to(path_origem), path_origem, path_semana)

    with zipfile.ZipFile(path_destino / 'horas_passadas' /(str(data_atual) + ".zip"), "w", compression=zipfile.ZIP_DEFLATED) as z:
        for arquivo in path_origem.rglob("*"):
            if arquivo.is_file():
                z.write(arquivo, arquivo.relative_to(path_origem))


if __name__ == '__main__':
    path_archive = Path('/home/joao/archive.cnpj.pw')
    #recriar_acumuladores_archive(path_archive)
    arquivar_csvs(Path('/home/joao/teste'), path_archive)
