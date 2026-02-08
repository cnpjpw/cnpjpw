import requests
from selectolax.lexbor import LexborHTMLParser
from datetime import datetime, timedelta
from tqdm import tqdm
import json
from concurrent.futures import ThreadPoolExecutor
from config import NUMERADOS, NAO_NUMERADOS


def get_infos_links(url_pasta, arquivos: list[str], data_template) -> dict:
    total = 0
    tamanhos = { arquivo: 0 for arquivo in arquivos }
    for arquivo in arquivos:
        link = url_pasta + arquivo
        res = requests.get(link, stream=True)
        tamanho = int(res.headers.get('content-length', 0))
        total += tamanho
        tamanhos[arquivo] = tamanho

    return { 'tamanhos': tamanhos, 'tamanho_total': total }


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


def verificar_existencia_pasta(ano, mes):
    LINK_BASE = 'https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/'
    URL_PASTA = LINK_BASE + str(ano) + '-' + str(mes).zfill(2) + '/'
    ULTIMA_MODIFICACAO_TEMPLATE = "%a, %d %b %Y %H:%M:%S %Z"
    res = requests.get(URL_PASTA)
    if res.status_code == 404:
        return False
    return True


def download_cnpj_zips(ano, mes, path_arquivos):
    def download_particao_thread(particao):
        for arquivo in particao:
            link = URL_PASTA + arquivo
            download_arquivo(arquivo, link, path_arquivos)
    LINK_BASE = 'https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/'
    URL_PASTA = LINK_BASE + str(ano) + '-' + str(mes).zfill(2) + '/'
    ULTIMA_MODIFICACAO_TEMPLATE = "%a, %d %b %Y %H:%M:%S %Z"
    res = requests.get(URL_PASTA)
    if res.status_code == 404:
        raise Exception('Pasta ainda não foi criada')
    arquivos = (
                [f'{arq_nome}.zip' for arq_nome in NAO_NUMERADOS] +
                [f'{arq_nome}{i}.zip' for arq_nome in NUMERADOS for i in range(10)]
                )
    infos = get_infos_links(URL_PASTA, arquivos, ULTIMA_MODIFICACAO_TEMPLATE)
    num_threads = round(infos['tamanho_total'] / max(infos['tamanhos'].values()))
    particoes = distribuir_arquivos_particoes(infos['tamanhos'], num_threads)
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(download_particao_thread, particoes)

