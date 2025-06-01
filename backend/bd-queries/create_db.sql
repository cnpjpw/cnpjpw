CREATE table empresas (
	cnpj_base CHAR(8) PRIMARY KEY,
	nome_empresarial VARCHAR(150),
	natureza_juridica SMALLINT REFERENCES naturezas_juridicas (codigo),
	qualificacao_responsavel SMALLINT,
	capital_social NUMERIC(17, 2),
	porte_empresa SMALLINT REFERENCES portes_empresas(codigo),
	ente_federativo VARCHAR(50)
);

CREATE table estabelecimentos (
	cnpj_base CHAR(8) REFERENCES empresas (cnpj_base),
	cnpj_ordem CHAR(4),
	cnpj_dv CHAR(2),
	identificador SMALLINT REFERENCES identificador_matriz_filial (codigo),
	nome_fantasia VARCHAR(55),
	situacao_cadastral SMALLINT REFERENCES situacoes_cadastrais (codigo),
	data_situacao_cadastral DATE,
	motivo_situacao_cadastral SMALLINT,
	nome_cidade_exterior VARCHAR(55),
	pais SMALLINT,
	data_inicio_atividade DATE,
	cnae_fiscal_principal INTEGER REFERENCES cnaes (codigo),
	cnaes_fiscais_secundarios INTEGER(7)[99],
	tipo_logradouro VARCHAR(20),
	logradouro VARCHAR(100),
	numero VARCHAR(6),
	complemento VARCHAR(156),
	bairro VARCHAR(100),
	cep VARCHAR(8),
	uf CHAR(2),
	municipio SMALLINT REFERENCES municipios (codigo),
	ddd1 CHAR(4),
	telefone_1 VARCHAR(9),
	ddd2 CHAR(4),
	telefone_2 VARCHAR(9),
	ddd_fax CHAR(4),
	fax VARCHAR(8),
	correio_eletronico VARCHAR(115),
	situacao_especial VARCHAR(25),
	data_situacao_especial DATE,
  PRIMARY KEY (cnpj_base, cnpj_ordem, cnpj_dv)
);

CREATE table dados_simples (
	cnpj_base CHAR(8) PRIMARY KEY,
	opcao_simples BOOLEAN,
	data_opcao_simples DATE,
	data_exclusao_simples DATE,
	opcao_mei BOOLEAN,
	data_opcao_mei DATE,
	data_exclusao_mei DATE
);

CREATE table socios (
	cnpj_base CHAR(8) REFERENCES empresas (cnpj_base),
	identificador SMALLINT REFERENCES identificadores_socios (codigo),
	nome VARCHAR(150),
	cnpj_cpf VARCHAR(14),
	qualificacao SMALLINT REFERENCES qualificacoes_socios (codigo),
	data_entrada_sociedade DATE,
	pais SMALLINT REFERENCES paises (codigo),
	cpf_representante CHAR(11),
	nome_representante VARCHAR(150),
	qualificacao_representante SMALLINT REFERENCES qualificacoes_representantes (codigo),
	faixa_etaria SMALLINT REFERENCES faixas_etarias (codigo)
);

CREATE table paises (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);


CREATE table municipios (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);


CREATE table qualificacoes_socios (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);

CREATE table naturezas_juridicas (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);


CREATE table cnaes (
	codigo INTEGER PRIMARY KEY,
	descricao VARCHAR(150)

);

CREATE table motivos_situacoes (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)

);

CREATE table faixas_etarias (
  codigo SMALLINT PRIMARY KEY,
  descricao VARCHAR(100)

);

CREATE table qualificacoes_representantes (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);


CREATE table identificador_matriz_filial (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);


CREATE table portes_empresas (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);

CREATE table situacoes_cadastrais (
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);


CREATE table identificadores_socios(
	codigo SMALLINT PRIMARY KEY,
	descricao VARCHAR(100)
);

