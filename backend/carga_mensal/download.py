import requests
from selectolax.lexbor import LexborHTMLParser
from datetime import datetime, timedelta
from tqdm import tqdm
import json


def parse_arquivos_pasta(html: str) -> list[str]:
    tree = LexborHTMLParser(html)
    arquivos = []

    links = tree.css('a')
    for link in links:
        endereco = link.attributes['href']
        if 'zip' in endereco:
            arquivos.append(endereco)
    return arquivos


def get_infos_links(links: list[str], data_template) -> dict:
    total = 0
    ultima_modificacao_pasta = timedelta(days=30)
    for link in links:
        res = requests.get(link, stream=True)
        total += int(res.headers.get('content-length', 0))
        ultima_modificacao_header = res.headers.get('last-modified')
        ultima_mod_data = datetime.strptime(ultima_modificacao_header, data_template)
        ultima_modificacao = datetime.now() - ultima_mod_data
        if ultima_modificacao < ultima_modificacao_pasta:
            ultima_modificacao_pasta = ultima_modificacao


    tamanho_total = round(total / 1_000_000_000, 2)
    return { 'tamanho_total': tamanho_total, 'ultima_modificacao': ultima_modificacao }


def download_arquivo(arquivo_nome, link, pasta_dir):
    res = requests.get(link, stream=True)
    total_size = int(res.headers.get("content-length", 0))
    block_size = 1024
    with tqdm(total=total_size, unit="B", unit_scale=True) as progress_bar:
        with open(pasta_dir + arquivo_nome, "wb") as file:
            for data in res.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)


def download_cnpj_zips(path_arquivos):
    pasta = path_arquivos + 'polling/'
    with open(pasta + "data_pasta.json") as f:
        data_pasta = json.load(f)

    ANO = data_pasta["ano"]
    MES = data_pasta["mes"]
    LINK_BASE = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'
    URL_PASTA = LINK_BASE + str(ANO) + '-' + str(MES).zfill(2) + '/'
    ULTIMA_MODIFICACAO_TEMPLATE = "%a, %d %b %Y %H:%M:%S %Z"

    res = requests.get(URL_PASTA)
    if res.status_code == 404:
        raise Exception('Pasta ainda não foi criada')
    arquivos = parse_arquivos_pasta(res.text)
    links_arquivos = [ URL_PASTA + arquivo for arquivo in arquivos ]
    infos = get_infos_links(links_arquivos, ULTIMA_MODIFICACAO_TEMPLATE)
    if infos['tamanho_total'] < 6:
        raise Exception('Pasta com tamanho menor que o esperado')
    if infos['ultima_modificacao'] < timedelta(seconds=3600):
        raise Exception('Pasta modificacada recentemente')
    for arquivo in arquivos:
        link = URL_PASTA + arquivo
        download_arquivo(arquivo, link, path_arquivos + str(MES).zfill(2) + '-' + str(ANO) + '/')

    data_pasta['mes'] += 1
    if data_pasta['mes'] == 13:
        data_pasta['mes'] = 1
        data_pasta['ano'] += 1

    with open(pasta + "data_pasta.json", "w") as f:
        json.dump(data_pasta, f)


