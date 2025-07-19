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
        'id': []
    }
}



AUXILIARES = ['Cnaes', 'Motivos', 'Municipios', 'Naturezas', 'Paises', 'Qualificacoes', 'Simples']
PRINCIPAIS = ['Empresas', 'Estabelecimentos', 'Socios']
NOMES_ARQUIVOS = AUXILIARES + [f'{p}{i}' for p in PRINCIPAIS for i in range(10)]

