--QUERY PARA SELECIONAR TODAS INFORMAÇÕES DE UM DADO CNPJ

SELECT row_to_json(result)
FROM (
    SELECT
        e.*,
        est.*,
        ds.opcao_simples,
        ds.data_opcao_simples,
        ds.data_exclusao_simples,
        ds.opcao_mei,
        ds.data_opcao_mei,
        ds.data_exclusao_mei,
        nj.descricao AS natureza_juridica_desc,
        ms.descricao AS motivo_situacao_desc,
        m.descricao AS municipio_desc,
        p.descricao AS pais_desc,
        c.descricao AS cnae_fiscal_principal_descricao,
        imf.descricao AS identificador_descricao,
        pe.descricao AS porte_empresa_descricao,
        sc.descricao AS situacao_cadastral_descricao,
        (
            SELECT json_agg(json_build_object(
                'id', s.socios_id,
                'identificador_entidade', s.identificador,
                'nome', s.nome,
                'cnpj_cpf', s.cnpj_cpf,
                'qualificacao_descricao', qs.descricao,
                'qualificacao_codigo', s.qualificacao,
                'data_entrada_sociedade', s.data_entrada_sociedade,
                'pais', ps.descricao,
                'cpf_representante', s.cpf_representante,
                'nome_representante', s.nome_representante,
                'qualificacao_representante_codigo', s.qualificacao_representante,
                'qualificacao_representante_descricao', qr.descricao,
                'faixa_etaria_codigo', s.faixa_etaria,
                'faixa_etaria_descricao', fe.descricao,
                'identificador_entidade_descricao', id_s.descricao
            ))
            FROM socios s
            LEFT JOIN qualificacoes_socios qs ON s.qualificacao = qs.codigo
            LEFT JOIN paises ps ON s.pais = ps.codigo
            LEFT JOIN identificadores_socios id_s ON s.identificador = id_s.codigo
            LEFT JOIN faixas_etarias fe ON fe.codigo = s.faixa_etaria
            LEFT JOIN qualificacoes_representantes qr ON qr.codigo = s.qualificacao_representante
            WHERE s.cnpj_base = est.cnpj_base
        ) AS socios,
        (
            SELECT json_agg(json_build_object(
                'codigo', cs.codigo,
                'descricao', c.descricao
            ))
            FROM unnest(est.cnaes_fiscais_secundarios) AS cs(codigo)
            LEFT JOIN cnaes c ON c.codigo = cs.codigo
        ) AS cnaes_fiscais_secundarios

    FROM estabelecimentos est
    JOIN empresas e ON est.cnpj_base = e.cnpj_base
    LEFT JOIN dados_simples ds ON est.cnpj_base = ds.cnpj_base
    LEFT JOIN naturezas_juridicas nj ON e.natureza_juridica = nj.codigo
    LEFT JOIN cnaes c ON est.cnae_fiscal_principal = c.codigo
    LEFT JOIN motivos_situacoes ms ON est.motivo_situacao_cadastral = ms.codigo
    LEFT JOIN municipios m ON est.municipio = m.codigo
    LEFT JOIN paises p ON est.pais = p.codigo
    LEFT JOIN identificador_matriz_filial imf ON est.identificador = imf.codigo
    LEFT JOIN portes_empresas pe ON pe.codigo = e.porte_empresa
    LEFT JOIN situacoes_cadastrais sc ON sc.codigo = est.situacao_cadastral

    WHERE est.cnpj_base=(%s)
      AND est.cnpj_ordem=(%s)
      AND est.cnpj_dv=(%s)
    ) result;

-- QUERY DATA ABERTURA

SELECT row_to_json(result) FROM (
  SELECT est.cnpj_base,
      est.cnpj_ordem,
      est.cnpj_dv,
      est.nome_fantasia,
      est.data_inicio_atividade,
      sc.descricao AS situacao_cadastral,
      e.nome_empresarial
  FROM estabelecimentos est
  LEFT JOIN empresas e ON est.cnpj_base = e.cnpj_base
  LEFT JOIN situacoes_cadastrais sc ON est.situacao_cadastral = sc.codigo
  WHERE data_inicio_atividade = (%s)::date
  ORDER BY (est.cnpj_base, est.cnpj_ordem, est.cnpj_dv) ASC LIMIT 25 OFFSET (%s)
) result;


-- QUERY FILTROS DIVERSOS

SELECT row_to_json(result) FROM (
  SELECT 
      est.cnpj_base,
      est.cnpj_ordem,
      est.cnpj_dv
      -- est.nome_fantasia,
      -- est.data_inicio_atividade,
      -- sc.descricao AS situacao_cadastral,
      -- e.nome_empresarial
  FROM estabelecimentos est
  LEFT JOIN empresas e ON est.cnpj_base = e.cnpj_base
  -- LEFT JOIN situacoes_cadastrais sc ON est.situacao_cadastral = sc.codigo
  WHERE (
      ( (%(data_abertura_min)s::date IS NULL) OR (est.data_inicio_atividade >= (%(data_abertura_min)s)::date) ) AND
      ( (%(data_abertura_max)s::date IS NULL) OR (est.data_inicio_atividade <= (%(data_abertura_max)s)::date) ) AND
      ( (%(razao_social)s::text IS NULL) OR (e.nome_empresarial LIKE UPPER(%(razao_social)s)) ) AND
      ( (%(cnpj_base)s::bpchar IS NULL) OR (est.cnpj_base = (%(cnpj_base)s)::bpchar) ) AND
      ( (%(situacao_cadastral)s::integer IS NULL) OR (est.situacao_cadastral = (%(situacao_cadastral)s)) ) AND
      ( (%(capital_social_min)s::numeric IS NULL) OR (e.capital_social >= (%(capital_social_min)s)) ) AND
      ( (%(capital_social_max)s::numeric IS NULL) OR (e.capital_social <= (%(capital_social_max)s)) ) AND
      ( (%(uf)s::bpchar IS NULL) OR (est.uf = (%(uf)s)) ) AND
      ( (%(municipio)s::integer IS NULL) OR (est.municipio = (%(municipio)s)) ) AND
      ( (%(cnae_principal)s::bpchar IS NULL) OR (est.cnae_fiscal_principal = (%(cnae_principal)s::bpchar)) ) AND
      ( (%(natureza_juridica)s::integer IS NULL) OR (e.natureza_juridica = (%(natureza_juridica)s)) )
  )
  ORDER BY est.cnpj_base, est.cnpj_ordem, est.cnpj_dv LIMIT 25 OFFSET (%(offset)s)
) result;

