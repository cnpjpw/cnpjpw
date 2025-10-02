CNPJ_QUERY = (
"""
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

    WHERE est.cnpj_base=(%s)::bpchar
    AND est.cnpj_ordem=(%s)::bpchar
    AND est.cnpj_dv=(%s)::bpchar
) result;

"""
)

RAZAO_QUERY = (
"""
SELECT row_to_json(result) FROM (
    SELECT e.cnpj_base,
        e.nome_empresarial
    FROM empresas e
    WHERE (
    e.nome_empresarial LIKE (%(razao_social)s || '%%') AND
    ( ((%(cursor)s)::bpchar IS NULL) OR (e.cnpj_base > (%(cursor)s)::bpchar) )
    )
    ORDER BY e.cnpj_base ASC LIMIT 25
) result;
"""
)

RAIZ_QUERY = (
"""
SELECT row_to_json(result) FROM (
    SELECT
        est.cnpj_base,
        est.cnpj_ordem,
        est.cnpj_dv,
        est.cnpj_base || est.cnpj_ordem || est.cnpj_dv AS cnpj,
        est.nome_fantasia,
        est.data_inicio_atividade,
        sc.descricao AS situacao_cadastral,
        e.nome_empresarial
    FROM estabelecimentos est
    LEFT JOIN empresas e ON est.cnpj_base = e.cnpj_base
    LEFT JOIN situacoes_cadastrais sc ON est.situacao_cadastral = sc.codigo
    WHERE (
        est.cnpj_base = (%(cnpj_base)s) AND
        ( ((%(cursor)s)::bpchar IS NULL) OR (est.cnpj_ordem > (%(cursor)s)::bpchar) )
    )
    ORDER BY est.cnpj_ordem ASC LIMIT 25
) result;
"""
)

DATA_ABERTURA_QUERY = (
"""
SELECT row_to_json(result) FROM (
    SELECT
        est.cnpj_base,
        est.cnpj_ordem,
        est.cnpj_dv,
        est.cnpj_base || est.cnpj_ordem || est.cnpj_dv AS cnpj,
        est.nome_fantasia,
        est.data_inicio_atividade,
        sc.descricao AS situacao_cadastral,
        e.nome_empresarial
    FROM estabelecimentos est
    LEFT JOIN empresas e ON est.cnpj_base = e.cnpj_base
    LEFT JOIN situacoes_cadastrais sc ON est.situacao_cadastral = sc.codigo
    WHERE (
        data_inicio_atividade = (%(data_inicio_atividade)s)::date AND
        (
        ((%(cnpj_base)s)::bpchar IS NULL) OR
        ((%(cnpj_ordem)s)::bpchar IS NULL) OR
        ((%(cnpj_dv)s)::bpchar IS NULL) OR
        ((est.cnpj_base, est.cnpj_ordem, est.cnpj_dv) > ( (%(cnpj_base)s), (%(cnpj_ordem)s), (%(cnpj_dv)s) ))
        )

    )
    ORDER BY est.cnpj_base, est.cnpj_ordem, est.cnpj_dv ASC LIMIT 25
) result;
"""
)

def get_busca_difusa_query(tem_socios_param, somente_socios):
    SOCIOS_LIMIT = ''
    SOCIOS_JOIN = ''
    if somente_socios:
        SOCIOS_LIMIT = 'LIMIT 250'
    if tem_socios_param:
        SOCIOS_JOIN = 'JOIN socios_filtrados s ON e.cnpj_base = s.cnpj_base'
    BUSCA_DIFUSA_QUERY = (
    """
    WITH
    empresas_filtradas AS (
        SELECT
        cnpj_base,
        nome_empresarial
        FROM empresas e
        WHERE
            (%(razao_social)s IS NULL OR e.nome_empresarial LIKE %(razao_social)s || '%%')
            AND (%(capital_social_min)s IS NULL OR e.capital_social >= %(capital_social_min)s)
            AND (%(capital_social_max)s IS NULL OR e.capital_social <= %(capital_social_max)s)
            AND (%(natureza_juridica)s IS NULL OR e.natureza_juridica = %(natureza_juridica)s)
            AND (%(cnpj_base)s IS NULL OR (e.cnpj_base >= %(cnpj_base)s))
    ),

    estabelecimentos_filtrados AS (
        SELECT
        cnpj_base,
        cnpj_ordem,
        cnpj_dv
        FROM estabelecimentos
        WHERE
            (%(data_abertura_min)s IS NULL OR data_inicio_atividade >= %(data_abertura_min)s::date)
            AND (%(data_abertura_max)s IS NULL OR data_inicio_atividade <= %(data_abertura_max)s::date)
            AND (%(uf)s IS NULL OR uf = %(uf)s)
            AND (%(municipio)s IS NULL OR municipio = %(municipio)s)
            AND (%(cnae)s IS NULL OR cnae_fiscal_principal = %(cnae)s)
            AND (%(situacao_cadastral)s IS NULL OR situacao_cadastral = %(situacao_cadastral)s)
            AND (
                (
                (%(cnpj_base)s IS NULL) OR
                (%(cnpj_ordem)s IS NULL) OR
                (%(cnpj_dv)s IS NULL)
                ) OR
                (cnpj_base, cnpj_ordem, cnpj_dv) > (%(cnpj_base)s, %(cnpj_ordem)s, %(cnpj_dv)s)
            )
    ),
    socios_filtrados AS (
        SELECT
        cnpj_base
        FROM socios s
        WHERE
            ( ((%(cnpj_base)s)::bpchar IS NULL) OR (s.cnpj_base >= %(cnpj_base)s) )
            AND ( ((%(socio_doc)s)::bpchar IS NULL) OR (s.cnpj_cpf = %(socio_doc)s) )
            AND ( ((%(socio_nome)s)::bpchar IS NULL) OR (s.nome LIKE (%(socio_nome)s || '%%')) )
            ORDER BY cnpj_base
            """
            +
            SOCIOS_LIMIT
            +
            """
    )
    SELECT row_to_json(result)
    FROM (
        SELECT
            est.cnpj_base,
            est.cnpj_ordem,
            est.cnpj_dv,
            est.cnpj_base || est.cnpj_ordem || est.cnpj_dv AS cnpj,
            e.nome_empresarial
        FROM empresas_filtradas e
        JOIN estabelecimentos_filtrados est ON e.cnpj_base = est.cnpj_base
        """
        +
        SOCIOS_JOIN
        +
        """
        ORDER BY est.cnpj_base, est.cnpj_ordem, est.cnpj_dv
        LIMIT 250
    ) result;
    """
    )
    return BUSCA_DIFUSA_QUERY

SOCIOS_QUERY = (
"""
SELECT row_to_json(result) FROM (
    SELECT
        cnpj_base,
        nome,
        cnpj_cpf
    FROM socios
    WHERE (
    cnpj_cpf = (%(doc)s) AND
    ( ((%(cursor)s)::bpchar IS NULL) OR (cnpj_base > (%(cursor)s)::bpchar) )
    )
    ORDER BY cnpj_base ASC LIMIT 25
) result;
"""
)

MUNICIPIOS_QUERY = (
"""
SELECT row_to_json(result) FROM (
    SELECT
        descricao,
        codigo
    FROM municipios
    ORDER BY descricao
) result;
"""
)

CNAES_QUERY = (
"""
SELECT row_to_json(result) FROM (
    SELECT
        descricao,
        codigo
    FROM cnaes
    ORDER BY descricao
) result;
"""
)

NATUREZAS_QUERY = (
"""
SELECT row_to_json(result) FROM (
    SELECT
        descricao,
        codigo
    FROM naturezas_juridicas
    ORDER BY descricao
) result;
"""
)

COUNT_DATA_QUERY = "SELECT count(*) from estabelecimentos WHERE data_inicio_atividade = (%s)::date"
COUNT_RAIZ_QUERY = "SELECT count(*) from estabelecimentos WHERE cnpj_base = (%s)::bpchar"
COUNT_RAZAO_QUERY = "SELECT count(*) from empresas WHERE nome_empresarial LIKE ((%s)::bpchar || '%%')"

