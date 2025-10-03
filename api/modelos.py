from pydantic import BaseModel
from typing import List

class Count(BaseModel):
    total: int

class CnaesFiscaisSecundario(BaseModel):
    codigo: int
    descricao: str


class Socio(BaseModel):
    identificador_entidade: int
    nome: str
    cnpj_cpf: str
    qualificacao_descricao: str
    qualificacao_codigo: int
    data_entrada_sociedade: str
    pais: str | None = None
    cpf_representante: str | None = None
    nome_representante: str | None = None
    qualificacao_representante_codigo: int
    qualificacao_representante_descricao: str
    faixa_etaria_codigo: int
    faixa_etaria_descricao: str
    identificador_entidade_descricao: str


class Estabelecimento(BaseModel):
    cnpj_base: str
    nome_empresarial: str | None = None
    natureza_juridica: int
    qualificacao_responsavel: int
    capital_social: float
    porte_empresa: int
    ente_federativo: str | None = None
    cnpj_ordem: str
    cnpj_dv: str
    identificador: int
    nome_fantasia: str | None = None
    situacao_cadastral: int
    data_situacao_cadastral: str
    motivo_situacao_cadastral: int
    nome_cidade_exterior: str | None = None
    pais: str | None = None
    data_inicio_atividade: str
    cnae_fiscal_principal: int
    cnaes_fiscais_secundarios: List[CnaesFiscaisSecundario]
    tipo_logradouro: str
    logradouro: str
    numero: str
    complemento: str
    bairro: str
    cep: str
    uf: str
    municipio: int
    ddd1: str | None = None
    telefone_1: str | None = None
    ddd2: str | None = None
    telefone_2: str | None = None
    fax: str | None = None
    ddd_fax: str | None = None
    correio_eletronico: str | None = None
    situacao_especial: str | None = None
    data_situacao_especial: str | None = None
    opcao_simples: bool
    data_opcao_simples: str
    data_exclusao_simples: str
    opcao_mei: bool
    data_opcao_mei: str
    data_exclusao_mei: str
    natureza_juridica_desc: str
    motivo_situacao_desc: str
    municipio_desc: str
    pais_desc: str | None = None
    cnae_fiscal_principal_descricao: str
    identificador_descricao: str
    porte_empresa_descricao: str
    situacao_cadastral_descricao: str
    socios: List[Socio]


class ItemAuxiliar(BaseModel):
    descricao: str
    codigo: int


class Auxiliares(BaseModel):
    resultados: List[ItemAuxiliar]


class EstabelecimentoPaginacaoItem(BaseModel):
    cnpj_base: str
    cnpj_ordem: str
    cnpj_dv: str
    cnpj: str
    nome_empresarial: str | None = None


class EmpresaPaginacaoItem(BaseModel):
    cnpj_base: str
    nome_empresarial: str | None = None


class SocioPaginacaoItem(BaseModel):
    cnpj_base: str
    nome: str
    cnpj_cpf: str


class PaginacaoEstabelecimentos(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[EstabelecimentoPaginacaoItem]

class PaginacaoEmpresas(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[EmpresaPaginacaoItem]


class PaginacaoSocios(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[SocioPaginacaoItem]


