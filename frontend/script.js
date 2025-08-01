// Variáveis para controle da paginação
let currentPage = 1
let resultsPerPage = 25
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
    quantResultsPage = paginacao['resultados_paginacao'].length
    const startIndex = (currentPage - 1) * resultsPerPage;
   
    document.getElementById('showing-results').textContent = 'listagem'
    document.getElementById('total-results').textContent = '';
    
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
    
    // Botão "Próxima"
    const nextButton = document.createElement('button');
    nextButton.className = 'pagination-button';
    nextButton.textContent = 'Próxima';
    nextButton.addEventListener('click', () => {
        currentPage++;
        getPaginacao(pathAPI, queryParams, currentPage)
        .then(paginacao => displayResults(paginacao));
    });
    pagination.appendChild(nextButton);
    heightSearchForm = document.querySelector('.search-form.active').offsetHeight
    window.scroll({
      top: heightSearchForm + 200,
      behavior: "smooth"
    });
}

