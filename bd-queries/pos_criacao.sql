ALTER TABLE socios ADD COLUMN socios_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY;

CREATE INDEX estabelecimentos_pais_idx ON estabelecimentos(pais);
CREATE INDEX motivo_situacao_cadastral_idx ON estabelecimentos(motivo_situacao_cadastral);
CREATE INDEX idx_socios_cnpj_base ON socios (cnpj_base);
CREATE INDEX razao_social_idx ON empresas (nome_empresarial);
CREATE INDEX capital_social_idx ON empresas (capital_social);
CREATE INDEX razao_social_segmentos_texto_idx ON empresas (nome_empresarial text_pattern_ops, cnpj_base);
CREATE INDEX natureza_juridica_idx ON empresas (natureza_juridica);
CREATE INDEX capital_cnpj_empresas_idx ON empresas (capital_social, cnpj_base);
CREATE INDEX estabelecimentos_municipio_idx ON estabelecimentos (municipio);
CREATE INDEX data_abertura_idx ON estabelecimentos (data_inicio_atividade);
CREATE INDEX cnpj_e_data_inscricao_idx ON estabelecimentos (data_inicio_atividade, cnpj_base, cnpj_ordem, cnpj_dv);
CREATE INDEX cnae_fiscal_estabelecimentos_idx ON estabelecimentos (cnae_fiscal_principal);
CREATE INDEX cnpj_e_cnae_idx ON estabelecimentos (cnae_fiscal_principal, data_inicio_atividade, cnpj_base, cnpj_ordem, cnpj_dv);
CREATE INDEX data_cnae_estabelecimentos_idx ON estabelecimentos (data_inicio_atividade, cnae_fiscal_principal, cnpj_base, cnpj_ordem, cnpj_dv);
CREATE INDEX idx_est_cnae_sit_data ON estabelecimentos (cnae_fiscal_principal, situacao_cadastral, data_inicio_atividade) INCLUDE (cnpj_base, cnpj_ordem, cnpj_dv);
CREATE INDEX idx_est_full_query ON estabelecimentos (cnae_fiscal_principal, situacao_cadastral, data_inicio_atividade, cnpj_base, cnpj_ordem, cnpj_dv);
CREATE INDEX estabelecimentos_uf_idx ON estabelecimentos (uf);
CREATE INDEX uf_data_abertura_cnpj_estabelecimentos_idx ON estabelecimentos (uf, data_inicio_atividade, cnpj_base, cnpj_ordem, cnpj_dv);

VACUUM ANALYZE empresas;
VACUUM ANALYZE estabelecimentos;
VACUUM ANALYZE socios;

