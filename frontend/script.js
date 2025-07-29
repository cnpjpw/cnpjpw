// Variáveis para controle da paginação
let currentPage = 1
let resultsPerPage = 25
let totalResults = 0
let dataAbertura;
let path = '';
let queryParams = '';


let cnpjTab = document.querySelector('#tab-cnpj')
let dateTab = document.querySelector('#tab-data')
let raizTab = document.querySelector('#tab-raiz')
let queryTab = document.querySelector('#tab-razao')

cnpjTab.addEventListener('click', (e) => switchTab('cnpj'))
dateTab.addEventListener('click', (e) => switchTab('data'))
raizTab.addEventListener('click', (e) => switchTab('raiz'))
queryTab.addEventListener('click', (e) => switchTab('razao'))

async function getPaginacao(path, queryParametros, pagina) {
  res = await fetch('https://api.cnpj.pw/' + path + '?p=' + pagina + queryParametros)
  paginacaoJson = await res.json()
  return paginacaoJson;
}


async function getPaginacaoQuery(paramsQuery, pagina) {
  res = await fetch('https://api.cnpj.pw/query/?' + 'p=' + pagina + paramsQuery)
  paginacaoJson = await res.json()
  return paginacaoJson;
  }

function switchTab(tab) {
    document.querySelectorAll('.search-tab').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.search-form').forEach(el => el.classList.remove('active'));
    
    document.getElementById('tab-' + tab).classList.add('active');
    document.getElementById('form-' + tab).classList.add('active');
    
    document.getElementById('results-container').classList.remove('active');
}

function searchCNPJ() {
    const cnpjValue = document.getElementById('cnpj-input').value.trim();
    if (cnpjValue) {
      window.open('https://cnpj.pw/cnpj/?v=' + cnpjValue)
    }
}

async function searchByDate() {
    dataAbertura = document.getElementById('data-abertura-tab-input').value;
    dataAbertura = dataAbertura.split('-').reverse().join('-')
    currentPage = 1
    
    if (dataAbertura) {
        pathAPI = 'data/' + dataAbertura
        queryParams = ''

        paginacao = await getPaginacao(pathAPI, queryParams, currentPage)
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
    }
}


async function searchByRaiz() {
    cnpjBase = document.getElementById('raiz-input').value;
    currentPage = 1
    
    if (cnpjBase) {
        pathAPI = 'cnpj_base/' + cnpjBase 
        queryParams = ''
        paginacao = await getPaginacao(pathAPI, queryParams, 1)
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
        return
    }
}


async function searchByRazao() {
    razao = document.getElementById('razao-input').value;
    currentPage = 1
    
    if (razao) {
        pathAPI = 'razao_social/' + razao 
        queryParams = ''
        paginacao = await getPaginacao(pathAPI, queryParams, 1)
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
        return
    }
}



async function searchByQuery() {
    params = {
    data_abertura_min : document.getElementById('data-abertura-min').value,
    data_abertura_max : document.getElementById('data-abertura-max').value,
    razao_social : document.getElementById('razao-social').value,
    cnpj_base : document.getElementById('cnpj-base').value,
    situacao_cadastral : document.getElementById('situacao-cadastral').value,
    capital_social_min : document.getElementById('capital-social-min').value,
    capital_social_max : document.getElementById('capital-social-max').value,
    uf : document.getElementById('uf').value,
    municipio : document.getElementById('municipio').value,
    cnae_principal : document.getElementById('cnae').value,
    natureza_juridica : document.getElementById('natureza-juridica').value
    }

    currentPage = 1

    let paramsArr = Object.entries(params)
    queryParams = ''
    paramsArr.forEach(e => queryParams += e[1] != '' ? `&${e[0]}=${e[1]}` : '')
    pathAPI = 'query/'
    getPaginacao(pathAPI, queryParams, 1)
    .then(paginacao => displayResults(paginacao))
    .then(document.getElementById('results-container').classList.add('active'))
}

function displayResults(paginacao) {
    const resultsBody = document.getElementById('results-body');
    resultsBody.innerHTML = '';
    totalResults = paginacao['quantidade_total_resultados'];
    quantResultsPage = paginacao['resultados_paginacao'].length
    const startIndex = (currentPage - 1) * resultsPerPage;
    const endIndex = Math.min(startIndex + resultsPerPage, totalResults);
   
    if (totalResults != null) {
        document.getElementById('showing-results').textContent = 
            totalResults > 0 ? `${startIndex + 1}-${endIndex}` : '0-0';
        document.getElementById('total-results').textContent = totalResults;
    }
    else {
        document.getElementById('showing-results').textContent = 'listagem'
        document.getElementById('total-results').textContent = '';
    }
    
    // Adiciona os dados à tabela
    for (let i = 0; i < quantResultsPage; i++) {
        const item = paginacao['resultados_paginacao'][i];
        const row = document.createElement('tr');
	const cnpj = (
		item.cnpj_base +
		(item.cnpj_ordem || '') +
		(item.cnpj_dv || '')
	)
  linkCnpj = item.cnpj_ordem ? 'https://cnpj.pw/cnpj/?v=' : 'https://api.cnpj.pw/cnpj_base/'
        
        row.innerHTML = `
            <td><a href="${linkCnpj}${cnpj}" target="_blank" class="cnpj-link">${cnpj}</a></td>
            <td>${item.nome_empresarial ?? ""}</td>
            <td>${item.nome_fantasia ?? ""}</td>
            <td class="data-abertura-td">${item.data_inicio_atividade ?? ""}</td>
            <td class="${item.situacao_cadastral == 'ATIVA' ? 'situacao-ativa' : 'situacao-outros'}">${item.situacao_cadastral ?? ""}</td>`;
        
        resultsBody.appendChild(row);
    }
    
    // Atualiza a paginação
    updatePagination();
}

function updatePagination() {
    const pagination = document.getElementById('pagination');
    pagination.innerHTML = '';
    
    // Calcula o número total de páginas
    const totalPages = Math.ceil(totalResults / resultsPerPage);
    
    // Botão "Anterior"
    const prevButton = document.createElement('button');
    prevButton.className = `pagination-button ${currentPage === 1 ? 'disabled' : ''}`;
    prevButton.textContent = 'Anterior';
    prevButton.disabled = currentPage === 1;
    prevButton.addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            getPaginacao(pathAPI, queryParams, currentPage)
            .then((paginacao) => {
              displayResults(paginacao)
              }
            );
        }
    });
    pagination.appendChild(prevButton);
    
    // Lógica de exibição dos números de página
    // Mostra sempre as primeiras páginas, a página atual e as últimas páginas
    const pagesToShow = [];
    
    if (totalPages <= 7) {
        // Se houver 7 ou menos páginas, mostra todas
        for (let i = 1; i <= totalPages; i++) {
            pagesToShow.push(i);
        }
    } else {
        // Sempre mostra as páginas 1 e 2
        pagesToShow.push(1, 2);
        
        // Lógica para páginas do meio
        if (currentPage <= 4) {
            // Estamos nas primeiras páginas
            pagesToShow.push(3, 4, 5);
        } else if (currentPage >= totalPages - 3) {
            // Estamos nas últimas páginas
            pagesToShow.push(totalPages - 4, totalPages - 3, totalPages - 2);
        } else {
            // Estamos no meio
            pagesToShow.push(currentPage - 1, currentPage, currentPage + 1);
        }
        
        // Sempre mostra as duas últimas páginas
        if (totalPages) {
            pagesToShow.push(totalPages - 1, totalPages);
        }
        
        // Adiciona ellipsis onde necessário
        const uniquePages = [...new Set(pagesToShow)].sort((a, b) => a - b);
        
        // Resultado final com ellipsis
        const result = [];
        for (let i = 0; i < uniquePages.length; i++) {
            if (i > 0 && uniquePages[i] > uniquePages[i - 1] + 1) {
                result.push('...');
            }
            result.push(uniquePages[i]);
        }
        
        pagesToShow.length = 0;
        pagesToShow.push(...result);
    }
    
    // Adiciona botões de página
    for (const page of pagesToShow) {
        if (page === '...') {
            const ellipsis = document.createElement('span');
            ellipsis.className = 'pagination-ellipsis';
            ellipsis.textContent = '...';
            pagination.appendChild(ellipsis);
        } else {
            const pageButton = document.createElement('button');
            pageButton.className = `pagination-button ${currentPage === page ? 'active' : ''}`;
            pageButton.textContent = page;
            pageButton.addEventListener('click', () => {
                currentPage = page
                pageButton.className = `pagination-button ${currentPage === page ? 'loading' : ''}`;
                window.scroll({
                  top: heightSearchForm + 200,
                  behavior: "smooth"
                });
                  getPaginacao(pathAPI, queryParams, currentPage).then(
                  (paginacao) => displayResults(paginacao)
                )
            });
            pagination.appendChild(pageButton);
        }
    }

    
    // Botão "Próxima"
    const nextButton = document.createElement('button');
    nextButton.className = `pagination-button ${currentPage === totalPages ? 'disabled' : ''}`;
    nextButton.textContent = 'Próxima';
    nextButton.disabled = currentPage === totalPages || totalPages === 0;
    nextButton.addEventListener('click', () => {
        if (currentPage < totalPages || isNaN(totalPages)) {
            currentPage++;
            getPaginacao(pathAPI, queryParams, currentPage)
            .then(paginacao => displayResults(paginacao));
        }
    });
    pagination.appendChild(nextButton);
    heightSearchForm = document.querySelector('.search-form.active').offsetHeight
    window.scroll({
      top: heightSearchForm + 200,
      behavior: "smooth"
    });
}

