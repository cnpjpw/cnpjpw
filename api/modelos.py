from pydantic import BaseModel
from typing import List

class Count(BaseModel):
    total: int

class CnaesFiscaisSecundario(BaseModel):
    codigo: int
    descricao: str | None = None


class Socio(BaseModel):
    identificador_entidade: int | None = None
    nome: str | None = None
    cnpj_cpf: str | None = None
    qualificacao_descricao: str | None = None
    qualificacao_codigo: int | None = None
    data_entrada_sociedade: str | None = None
    pais: str | None = None
    cpf_representante: str | None = None
    nome_representante: str | None = None
    qualificacao_representante_codigo: int | None = None
    qualificacao_representante_descricao: str | None = None
    faixa_etaria_codigo: int | None = None
    faixa_etaria_descricao: str | None = None
    identificador_entidade_descricao: str | None = None


class Estabelecimento(BaseModel):
    cnpj_base: str
    nome_empresarial: str | None = None
    natureza_juridica: int | None = None
    qualificacao_responsavel: int | None = None
    capital_social: float
    porte_empresa: int | None = None
    ente_federativo: str | None = None
    cnpj_ordem: str
    cnpj_dv: str
    identificador: int | None = None
    nome_fantasia: str | None = None
    situacao_cadastral: int | None = None
    data_situacao_cadastral: str | None = None
    motivo_situacao_cadastral: int | None = None
    nome_cidade_exterior: str | None = None
    pais: str | None = None
    data_inicio_atividade: str | None = None
    cnae_fiscal_principal: int | None = None
    cnaes_fiscais_secundarios: List[CnaesFiscaisSecundario] | None = None
    tipo_logradouro: str | None = None
    logradouro: str | None = None
    numero: str | None = None
    complemento: str | None = None
    bairro: str | None = None
    cep: str | None = None
    uf: str | None = None
    municipio: int | None = None
    ddd1: str | None = None
    telefone_1: str | None = None
    ddd2: str | None = None
    telefone_2: str | None = None
    fax: str | None = None
    ddd_fax: str | None = None
    correio_eletronico: str | None = None
    situacao_especial: str | None = None
    data_situacao_especial: str | None = None
    opcao_simples: bool | None = None
    data_opcao_simples: str | None = None
    data_exclusao_simples: str | None = None
    opcao_mei: bool | None = None
    data_opcao_mei: str | None = None
    data_exclusao_mei: str | None = None
    natureza_juridica_desc: str | None = None
    motivo_situacao_desc: str | None = None
    municipio_desc: str | None = None
    pais_desc: str | None = None
    cnae_fiscal_principal_descricao: str | None = None
    identificador_descricao: str | None = None
    porte_empresa_descricao: str | None = None
    situacao_cadastral_descricao: str | None = None
    socios: List[Socio] | None


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
    nome: str | None = None
    cnpj_cpf: str | None = None


class PaginacaoEstabelecimentos(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[EstabelecimentoPaginacaoItem]

class PaginacaoEmpresas(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[EmpresaPaginacaoItem]


class PaginacaoSocios(BaseModel):
    limite_resultados_paginacao: int
    resultados_paginacao: List[SocioPaginacaoItem]


