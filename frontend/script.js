// Variáveis para controle da paginação
let resultsPerPage = 25
let dataAbertura;
let path = '';
let cursor = '';


let cnpjTab = document.querySelector('#tab-cnpj')
let dateTab = document.querySelector('#tab-data')
let raizTab = document.querySelector('#tab-raiz')
let queryTab = document.querySelector('#tab-razao')

cnpjTab.addEventListener('click', (e) => switchTab('cnpj'))
dateTab.addEventListener('click', (e) => switchTab('data'))
raizTab.addEventListener('click', (e) => switchTab('raiz'))
queryTab.addEventListener('click', (e) => switchTab('razao'))

async function getPaginacao(path, cursor) {
  cursorParametro = cursor ? '?cursor=' + cursor : ''
  res = await fetch('https://api.cnpj.pw/' + path + cursorParametro)
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
      window.open('https://api.cnpj.pw/cnpj/' + cnpjValue)
    }
}

async function searchByDate() {
    dataAbertura = document.getElementById('data-abertura-tab-input').value;
    dataAbertura = dataAbertura.split('-').reverse().join('-')
    cursor = 1
    
    if (dataAbertura) {
        pathAPI = 'data2/' + dataAbertura

        paginacao = await getPaginacao(pathAPI, '')
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
    }
}


async function searchByRaiz() {
    cnpjBase = document.getElementById('raiz-input').value;
    cursor = 1
    
    if (cnpjBase) {
        pathAPI = 'cnpj_base2/' + cnpjBase 
        paginacao = await getPaginacao(pathAPI, '')
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
        return
    }
}


async function searchByRazao() {
    razao = document.getElementById('razao-input').value;
    cursor = 1
    tipoPaginacao = 2;
    
    if (razao) {
        pathAPI = 'razao_social2/' + razao 
        paginacao = await getPaginacao(pathAPI, '')
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
        return
    }
}


function displayResults(paginacao) {
    const resultsBody = document.getElementById('results-body');
    resultsBody.innerHTML = '';
    quantResultsPage = paginacao['resultados_paginacao'].length
   
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
  linkCnpj = item.cnpj_ordem ? 'https://api.cnpj.pw/cnpj/' : 'https://api.cnpj.pw/cnpj_base2/'
        
        row.innerHTML = `
            <td><a href="${linkCnpj}${cnpj}" target="_blank" class="cnpj-link">${cnpj}</a></td>
            <td>${item.nome_empresarial ?? ""}</td>
            <td>${item.nome_fantasia ?? ""}</td>
            <td class="data-abertura-td">${item.data_inicio_atividade ?? ""}</td>
            <td class="${item.situacao_cadastral == 'ATIVA' ? 'situacao-ativa' : 'situacao-outros'}">${item.situacao_cadastral ?? ""}</td>`;
        
        resultsBody.appendChild(row);
    }
    
    // Atualiza a paginação
    updatePagination(paginacao);
}

function updatePagination(paginacao) {
    const pagination = document.getElementById('pagination');
    pagination.innerHTML = '';
    
    const nextButton = document.createElement('button');
    nextButton.className = 'pagination-button';
    nextButton.textContent = 'Próxima';
    nextButton.addEventListener('click', () => {
        tam = paginacao['resultados_paginacao'].length
        ultimo_res = paginacao['resultados_paginacao'][tam - 1]
        cnpj_base = ultimo_res['cnpj_base']
        cnpj_ordem = ultimo_res['cnpj_ordem']
        cnpj_dv = ultimo_res['cnpj_dv']

        if (pathAPI.startsWith('razao_social2')) {
          cursor = cnpj_base
        }

        if (pathAPI.startsWith('cnpj_base2')) {
          cursor = cnpj_ordem
        }

        if (pathAPI.startsWith('data2')) {
          cursor = cnpj_base + cnpj_ordem + cnpj_dv
        }

        getPaginacao(pathAPI, cursor)
        .then(pag => displayResults(pag));
    });

    pagination.appendChild(nextButton);
    heightSearchForm = document.querySelector('.search-form.active').offsetHeight
    window.scroll({
      top: heightSearchForm + 200,
      behavior: "smooth"
    });
}

