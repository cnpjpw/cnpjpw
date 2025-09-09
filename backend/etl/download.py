import requests
from selectolax.lexbor import LexborHTMLParser
from datetime import datetime, timedelta
from tqdm import tqdm
import json
from concurrent.futures import ThreadPoolExecutor


def parse_arquivos_pasta(html: str) -> list[str]:
    tree = LexborHTMLParser(html)
    arquivos = []

    links = tree.css('a')
    for link in links:
        endereco = link.attributes['href']
        if 'zip' in endereco:
            arquivos.append(endereco)
    return arquivos


def get_infos_links(url_pasta, arquivos: list[str], data_template) -> dict:
    total = 0
    ultima_modificacao_pasta = timedelta(days=30)
    tamanhos = { arquivo: 0 for arquivo in arquivos }
    for arquivo in arquivos:
        link = url_pasta + arquivo
        res = requests.get(link, stream=True)
        tamanho = int(res.headers.get('content-length', 0))
        total += tamanho
        tamanhos[arquivo] = tamanho
        ultima_modificacao_header = res.headers.get('last-modified')
        ultima_mod_data = datetime.strptime(ultima_modificacao_header, data_template)
        ultima_modificacao = datetime.now() - ultima_mod_data
        if ultima_modificacao < ultima_modificacao_pasta:
            ultima_modificacao_pasta = ultima_modificacao

    return { 'tamanhos': tamanhos, 'tamanho_total': total, 'ultima_modificacao': ultima_modificacao }


def download_arquivo(arquivo_nome, link, pasta_dir):
    with requests.get(link, stream=True) as r:
        r.raise_for_status()
        with open(pasta_dir / arquivo_nome, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def distribuir_arquivos_particoes(arquivos_tamanhos: dict, num_threads):
    arquivos_tamanhos = list(arquivos_tamanhos.items())
    arquivos_ordenados = sorted(arquivos_tamanhos, reverse=True, key=lambda x: x[1])
    cargas = [0] * num_threads
    particoes = [[] for _ in range(num_threads)]
    for arq, tamanho in arquivos_ordenados:
        idx = cargas.index(min(cargas))
        particoes[idx].append(arq)
        cargas[idx] += tamanho
    return particoes


def download_cnpj_zips(ano, mes, path_arquivos):
    def download_particao_thread(particao):
        for arquivo in particao:
            link = URL_PASTA + arquivo
            download_arquivo(arquivo, link, path_arquivos)
    LINK_BASE = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'
    URL_PASTA = LINK_BASE + str(ano) + '-' + str(mes).zfill(2) + '/'
    ULTIMA_MODIFICACAO_TEMPLATE = "%a, %d %b %Y %H:%M:%S %Z"
    res = requests.get(URL_PASTA)
    if res.status_code == 404:
        raise Exception('Pasta ainda não foi criada')
    arquivos = parse_arquivos_pasta(res.text)
    infos = get_infos_links(URL_PASTA, arquivos, ULTIMA_MODIFICACAO_TEMPLATE)
    if len(arquivos) < 37:
        raise Exception('Pasta com menos arquivos que o esperado')
    if infos['ultima_modificacao'] < timedelta(seconds=600):
        raise Exception('Pasta modificacada recentemente')
    num_threads = round(infos['tamanho_total'] / max(infos['tamanhos'].values()))
    particoes = distribuir_arquivos_particoes(infos['tamanhos'], num_threads)
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(download_particao_thread, particoes)

