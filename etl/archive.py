import shutil
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import zipfile
from config import PRINCIPAIS, ARQ_TABELA_DIC


def extrair_arquivo_zip(path_zip, destino):
    with zipfile.ZipFile(path_zip, 'r') as archive:
        arqs_internos = archive.namelist()
        for nome_interno in arqs_internos:
            archive.extract(nome_interno, path=destino)


def recriar_acumuladores_archive(archive_horas, archive_dias, archive_semanas, dir_tmp, data_inicial):
    nomes_zips = [z.parts[-1] for z in archive_horas.iterdir()]
    for nome_zip in nomes_zips:
        data_zip = datetime.fromisoformat(nome_zip.split('.zip')[0])
        pasta_dia, pasta_semana = encontrar_pastas_archive(data_zip, data_inicial)
        extrair_arquivo_zip(archive_horas / nome_zip, dir_tmp)
        path_dia_atual = archive_dias / pasta_dia
        path_semana_atual = archive_semanas / pasta_semana
        paths = [path_dia_atual, path_semana_atual]
        for p in paths:
            p.mkdir(parents=True, exist_ok=True)
        for file in dir_tmp.iterdir():
            nome = file.parts[-1]
            acumular_csv(file, path_dia_atual / nome)
            acumular_csv(file, path_semana_atual / nome)
            file.unlink()

def renomear_zips_archive(archive_horas):
    nomes_zips = [z.parts[-1] for z in archive_horas.iterdir()]
    for nome_zip in nomes_zips:
        data_zip = nome_zip.split('.zip')[0]
        (archive_horas / data_zip).mkdir(exist_ok=True)
        pasta_tmp = (archive_horas / data_zip)
        extrair_arquivo_zip(archive_horas / nome_zip, pasta_tmp)
        nomes_antigos = [arquivo.parts[-1] for arquivo in pasta_tmp.iterdir()]
        for nome_antigo in nomes_antigos:
            arquivo = pasta_tmp / nome_antigo
            nome_tabela = ARQ_TABELA_DIC[nome_antigo.split('.')[0]]
            arquivo.rename(pasta_tmp / f'{nome_tabela}.csv')
        (archive_horas / nome_zip).unlink()
    compactar_pastas(archive_horas)


def compactar_pastas(path_periodo):
    for file in path_periodo.iterdir():
        with zipfile.ZipFile(path_periodo / (file.parts[-1] + ".zip"), "w", compression=zipfile.ZIP_DEFLATED) as z:
            for arquivo in file.rglob("*"):
                if arquivo.is_file():
                    z.write(arquivo, arquivo.relative_to(file))
        shutil.rmtree(file)


def acumular_csv(path_entrada, path_saida):
    with open(path_saida, 'ab') as wfd, \
         open(path_entrada, 'rb') as fd:
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


def arquivar_csvs(path_origem, dic_origem_destino, path_horas, path_dias, path_semanas, data_inicial):
    data_atual = datetime.now(tz=timezone(timedelta(hours=-3)))
    pasta_dia, pasta_semana = encontrar_pastas_archive(data_atual, data_inicial)
    path_dia = path_dias / pasta_dia
    path_semana = path_semanas / pasta_semana
    path_dia.mkdir(parents=True, exist_ok=True)
    path_semana.mkdir(parents=True, exist_ok=True)
    compactar_archives_passados(path_dias, pasta_dia)
    compactar_archives_passados(path_semanas, pasta_semana)
    for arquivo in path_origem.rglob("*"):
        nome = arquivo.parts[-1].split('.')[0]
        nome_destino = dic_origem_destino[nome]
        print(nome_destino)
        acumular_csv(arquivo, path_dia / f'{nome_destino}.csv')
        acumular_csv(arquivo, path_semana / f'{nome_destino}.csv')

    with zipfile.ZipFile(path_horas / (str(data_atual) + ".zip"), "w", compression=zipfile.ZIP_DEFLATED) as z:
        for arquivo in path_origem.rglob("*"):
            nome = arquivo.parts[-1].split('.')[0]
            nome_destino = dic_origem_destino[nome]
            print(nome_destino)
            if arquivo.is_file():
                z.write(arquivo, f'{nome_destino}.csv')

