// Variáveis para controle da paginação
let resultsPerPage = 25
let dataAbertura;
let path = '';
let cursor = '';
let total = 0;
let queryParametros = new URLSearchParams();


let cnpjTab = document.querySelector('#tab-cnpj')
let dateTab = document.querySelector('#tab-data')
let raizTab = document.querySelector('#tab-raiz')
let queryTab = document.querySelector('#tab-razao')


let selectElement = document.querySelector('.search-tabs')

selectElement.addEventListener('change', (e) => {
    tabName = e.target.selectedOptions[0].getAttribute('tab')
    switchTab(tabName)


})

/*
cnpjTab.addEventListener('select', (e) => switchTab('cnpj'))
dateTab.addEventListener('select', (e) => switchTab('data'))
raizTab.addEventListener('select', (e) => switchTab('raiz'))
queryTab.addEventListener('select', (e) => switchTab('razao'))
*/

async function getPaginacao(path, cursor, queryParametros) {
  queryParametros.delete('cursor')
  if (cursor) {
      queryParametros.set('cursor', cursor)
  }
  res = await fetch('https://api.cnpj.pw/' + path + '?' +queryParametros.toString())
  paginacaoJson = await res.json()
  return paginacaoJson;
}


async function getCount(path) {
  res = await fetch('https://api.cnpj.pw/count/' + path)
  totalJson = await res.json()
  return totalJson['total'];
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
    queryParametros = new URLSearchParams();
    dataAbertura = document.getElementById('data-abertura-tab-input').value;
    dataAbertura = dataAbertura.split('-').reverse().join('-')
    cursor = ''
    cursorHistory = []

    if (dataAbertura) {
        total = 0
        pathAPI = 'data/' + dataAbertura

        paginacao = await getPaginacao(pathAPI, '', queryParametros)
        total = await getCount(pathAPI)
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
    }
}

async function searchByQuery() {
    total = 0
    cursor = ''
    cursorHistory = []

    params = {
    data_abertura_min : document.getElementById('data-abertura-min').value,
    data_abertura_max : document.getElementById('data-abertura-max').value,
    razao_social : document.getElementById('razao-social').value,
    situacao_cadastral : document.getElementById('situacao-cadastral').value,
    capital_social_min : document.getElementById('capital-social-min').value,
    capital_social_max : document.getElementById('capital-social-max').value,
    estado : document.getElementById('uf').value,
    municipio : document.getElementById('municipio').value,
    cnae : document.getElementById('cnae').value,
    natureza_juridica : document.getElementById('natureza-juridica').value,
    socio_doc : document.getElementById('socio-doc').value,
    socio_nome : document.getElementById('socio-nome').value
    }


    if (params.data_abertura_min) {
        data = params.data_abertura_min
        params.data_abertura_min  = data.split('-').reverse().join('-')
    }
    if (params.data_abertura_max) {
        data = params.data_abertura_max
        params.data_abertura_max  = data.split('-').reverse().join('-')
    }

    queryParametros = new URLSearchParams()
    chavesParametros = Object.keys(params)
    chavesParametros.forEach((chave) => {
        if (!(params[chave] === null || params[chave] === '')) {
            queryParametros.set(chave, params[chave])
        }
    })

    pathAPI = 'busca_difusa/'
    getPaginacao(pathAPI, '', queryParametros)
    .then(paginacao => displayResults(paginacao))
    .then(document.getElementById('results-container').classList.add('active'))
}

async function searchByRaiz() {
    cnpjBase = document.getElementById('raiz-input').value;
    cursor = ''
    queryParametros = new URLSearchParams();
    cursorHistory = []

    if (cnpjBase) {
        total = 0
        pathAPI = 'cnpj_base/' + cnpjBase
        total = await getCount(pathAPI)
        paginacao = await getPaginacao(pathAPI, '', queryParametros)
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
        return
    }
}


async function searchByRazao() {
    razao = document.getElementById('razao-input').value;
    cursor = ''
    queryParametros = new URLSearchParams();
    cursorHistory = []

    if (razao) {
        total = 0
        pathAPI = 'razao_social/' + razao
        paginacao = await getPaginacao(pathAPI, '', queryParametros)
        displayResults(paginacao)
        document.getElementById('results-container').classList.add('active')
        total = await getCount(pathAPI)
        displayCount(total)
        return
    }
}

function displayCount(total) {
    document.getElementById('showing-results').textContent = 'listagem'
    document.getElementById('total-results').textContent = total ? total : '';
}


function displayResults(paginacao) {
    const resultsBody = document.getElementById('results-body');
    resultsBody.innerHTML = '';
    quantResultsPage = paginacao['resultados_paginacao'].length

    displayCount(total)

    // Adiciona os dados à tabela
    for (let i = 0; i < quantResultsPage; i++) {
        const item = paginacao['resultados_paginacao'][i];
        const row = document.createElement('tr');
	const cnpj = (
		item.cnpj_base +
		(item.cnpj_ordem || '') +
		(item.cnpj_dv || '')
	)
  linkCnpj = item.cnpj_ordem ? 'https://api.cnpj.pw/cnpj/' : 'https://api.cnpj.pw/cnpj_base/'

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
    if (paginacao['resultados_paginacao'].length < 25) {
        nextButton.classList.add('disabled')
    }
    else {
      nextButton.addEventListener('click', () => {
          tam = paginacao['resultados_paginacao'].length
          ultimo_res = paginacao['resultados_paginacao'][tam - 1]
          cnpj_base = ultimo_res['cnpj_base']
          cnpj_ordem = ultimo_res['cnpj_ordem']
          cnpj_dv = ultimo_res['cnpj_dv']
          cursorHistory.push(cursor)

          if (pathAPI.startsWith('razao_social')) {
            cursor = cnpj_base
          }

          if (pathAPI.startsWith('cnpj_base')) {
            cursor = cnpj_ordem
          }

          if (pathAPI.startsWith('busca_difusa')) {
            cursor = cnpj_base
          }

          if (pathAPI.startsWith('data')) {
            cursor = cnpj_base + cnpj_ordem + cnpj_dv
          }

          getPaginacao(pathAPI, cursor, queryParametros)
          .then(pag => displayResults(pag));
      });
    }

    const prevButton = document.createElement('button');
    prevButton.className = 'pagination-button';
    prevButton.textContent = 'Anterior';

    if (cursorHistory.length === 0) {
        prevButton.classList.add('disabled')
    }
    else {
      prevButton.addEventListener('click', () => {
          cursor = cursorHistory.pop()
          getPaginacao(pathAPI, cursor, queryParametros)
          .then(pag => displayResults(pag));
      });
    }

    pagination.appendChild(prevButton);
    pagination.appendChild(nextButton);
    heightSearchForm = document.querySelector('.search-form.active').offsetHeight
    window.scroll({
      top: heightSearchForm + 200,
      behavior: "smooth"
    });
}
