TIPOS_INDICES = {
    'Empresas': {
        'date': [],
        'numeric': [4],
        'bool': [],
        'array': [],
        'id': [0]
    },
    'Estabelecimentos': {
        'date': [6, 10, 29],
        'numeric': [],
        'bool': [],
        'array': [12],
        'id': [0, 1, 2]
    },
    'Simples': {
        'date': [2, 3, 5, 6],
        'numeric': [],
        'bool': [1, 4],
        'array': [],
        'id': [0]
    },
    'Socios': {
        'date': [5],
        'numeric': [],
        'bool': [],
        'array': [],
        'id': []
    },

    'Outros': {
        'date': [],
        'numeric': [],
        'bool': [],
        'array': [],
        'id': [0]
    }
}



AUXILIARES = ['Cnaes', 'Motivos', 'Municipios', 'Naturezas', 'Paises', 'Qualificacoes']
PRINCIPAIS = ['Empresas', 'Socios', 'Estabelecimentos', 'Simples']

NUMERADOS = ['Empresas', 'Socios', 'Estabelecimentos']
NAO_NUMERADOS = ['Cnaes', 'Motivos', 'Municipios', 'Naturezas', 'Paises', 'Qualificacoes', 'Simples']
NOMES_ARQUIVOS = NAO_NUMERADOS + [f'{nome}{i}' for nome in NUMERADOS for i in range(10)]

ARQ_TABELA_DIC = {
        'Cnaes': 'cnaes',
        'Motivos': 'motivos_situacoes',
        'Municipios': 'municipios', 
        'Naturezas': 'naturezas_juridicas',
        'Paises': 'paises',
        'Qualificacoes': 'qualificacoes_socios',
        'Simples': 'dados_simples',
        'Empresas': 'empresas',
        'Estabelecimentos': 'estabelecimentos',
        'Socios': 'socios'
        }


tabelas_infos = {
    'empresas':
    {'colunas':
     [
        'nome_empresarial',
        'natureza_juridica',
        'qualificacao_responsavel',
        'capital_social',
        'porte_empresa',
        'ente_federativo'

    ],
     'pk': ['cnpj_base']
     },
    'estabelecimentos':
    {'colunas': [
        'identificador',
        'nome_fantasia',
        'situacao_cadastral',
        'data_situacao_cadastral',
        'motivo_situacao_cadastral',
        'nome_cidade_exterior',
        'pais',
        'data_inicio_atividade',
        'cnae_fiscal_principal',
        'cnaes_fiscais_secundarios',
        'tipo_logradouro',
        'logradouro',
        'numero',
        'complemento',
        'bairro',
        'cep',
        'uf',
        'municipio',
        'ddd1',
        'telefone_1',
        'ddd2',
        'telefone_2',
        'ddd_fax',
        'fax',
        'correio_eletronico',
        'situacao_especial',
        'data_situacao_especial'
    ],
    'pk': [
        'cnpj_base',
        'cnpj_ordem',
        'cnpj_dv'
        ]
    },

    'dados_simples':
    {
        'colunas': [
        'opcao_simples', 
        'data_opcao_simples', 
        'data_exclusao_simples',
        'opcao_mei',
        'data_opcao_mei',
        'data_exclusao_mei'
    ],
        'pk': ['cnpj_base']
     },

    'socios': {
        'colunas': [
        'identificador',
        'qualificacao',
        'data_entrada_sociedade',
        'pais',
        'cpf_representante',
        'nome_representante',
        'qualificacao_representante',
        'faixa_etaria'
    ],
    'pk': ['cnpj_base', 'nome', 'cnpj_cpf']
    },

    'auxiliares': {
        'colunas': ['descricao'],
        'pk': ['codigo']

    }
}
