-- ==============================================================
-- DATASUS Allergy Pipeline — Inicialização do Banco de Dados
-- Executa automaticamente quando o container Postgres é criado.
-- Estrutura: 3 schemas (Medallion Architecture)
--   raw     → Bronze: dados brutos sem transformação
--   dwh     → Silver: modelo estrela limpo e tipado
--   analytics → Gold: KPIs prontos para consumo
-- ==============================================================

-- ==============================================================
-- SCHEMAS
-- ==============================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;


-- ==============================================================
-- CAMADA BRONZE (raw)
-- Armazena os dados exatamente como vieram do DATASUS/PySUS.
-- Colunas são todas TEXT para evitar erros de tipo na ingestão.
-- ==============================================================
CREATE TABLE IF NOT EXISTS raw.internacoes (
    -- Controle de carga
    id              SERIAL PRIMARY KEY,
    competencia     VARCHAR(6),    -- AAAAMM, ex: 202501
    loaded_at       TIMESTAMP DEFAULT NOW(),

    -- Identificação
    n_aih           TEXT,          -- Número da AIH
    uf_zi           TEXT,          -- UF do hospital
    mun_res         TEXT,          -- Código IBGE município do paciente
    mun_mov         TEXT,          -- Código IBGE município do atendimento
    cnes            TEXT,          -- CNES do hospital

    -- Diagnóstico
    diag_princ      TEXT,          -- CID principal
    diag_secun      TEXT,          -- CID secundário
    cobranca        TEXT,          -- Código de cobrança

    -- Paciente
    sexo            TEXT,
    idade           TEXT,
    nacional        TEXT,          -- Nacionalidade

    -- Internação
    dt_inter        TEXT,          -- Data de internação (AAAAMMDD)
    dt_saida        TEXT,          -- Data de saída
    dias_perm       TEXT,          -- Dias de permanência
    morte           TEXT,          -- 0=não, 1=sim

    -- Financeiro
    val_tot         TEXT,          -- Valor total pago (R$)
    val_sh          TEXT,          -- Valor serviços hospitalares
    val_sp          TEXT,          -- Valor serviços profissionais
    val_uti         TEXT,          -- Valor UTI
    qt_diarias      TEXT,          -- Qtd de diárias

    -- Procedimento
    proc_rea        TEXT,          -- Procedimento realizado
    proc_solic      TEXT           -- Procedimento solicitado
);

-- Índice para queries por competência e diagnóstico
CREATE INDEX IF NOT EXISTS idx_raw_competencia    ON raw.internacoes(competencia);
CREATE INDEX IF NOT EXISTS idx_raw_diag_princ     ON raw.internacoes(diag_princ);


-- ==============================================================
-- CAMADA SILVER (dwh) — Star Schema
-- Filtra apenas CIDs de alergia/asma: J45, J30, L20, T78
-- ==============================================================

-- Dimensão Tempo
CREATE TABLE IF NOT EXISTS dwh.dim_tempo (
    sk_tempo        SERIAL PRIMARY KEY,
    competencia     VARCHAR(6) UNIQUE NOT NULL,  -- AAAAMM
    ano             SMALLINT NOT NULL,
    mes             SMALLINT NOT NULL,
    trimestre       SMALLINT NOT NULL,
    semestre        SMALLINT NOT NULL,
    nome_mes        VARCHAR(20) NOT NULL
);

-- Dimensão Localidade
CREATE TABLE IF NOT EXISTS dwh.dim_localidade (
    sk_localidade   SERIAL PRIMARY KEY,
    cod_ibge        VARCHAR(7) UNIQUE NOT NULL,
    nome_municipio  VARCHAR(100),
    uf              VARCHAR(2),
    regiao          VARCHAR(20),
    latitude        NUMERIC(10,6),
    longitude       NUMERIC(10,6)
);

-- Dimensão Diagnóstico
CREATE TABLE IF NOT EXISTS dwh.dim_diagnostico (
    sk_diagnostico  SERIAL PRIMARY KEY,
    cid             VARCHAR(5) UNIQUE NOT NULL,
    descricao       VARCHAR(200),
    grupo           VARCHAR(100),   -- Ex: "Asma", "Rinite", "Dermatite", "Anafilaxia"
    subcategoria    VARCHAR(50)
);

-- Tabela Fato — Internações por Alergia/Asma
CREATE TABLE IF NOT EXISTS dwh.fct_internacoes (
    sk_internacao   SERIAL PRIMARY KEY,

    -- Chaves estrangeiras para dimensões
    sk_tempo        INT REFERENCES dwh.dim_tempo(sk_tempo),
    sk_localidade   INT REFERENCES dwh.dim_localidade(sk_localidade),
    sk_diagnostico  INT REFERENCES dwh.dim_diagnostico(sk_diagnostico),

    -- Atributos degenerados (não viram dimensão)
    n_aih           VARCHAR(20),
    cnes            VARCHAR(10),
    sexo            CHAR(1),         -- M / F
    idade_anos      SMALLINT,
    morte           BOOLEAN,

    -- Métricas
    dias_permanencia SMALLINT,
    val_total       NUMERIC(12,2),
    val_uti         NUMERIC(12,2),

    -- Auditoria
    competencia     VARCHAR(6),
    inserted_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fct_tempo        ON dwh.fct_internacoes(sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fct_localidade   ON dwh.fct_internacoes(sk_localidade);
CREATE INDEX IF NOT EXISTS idx_fct_diagnostico  ON dwh.fct_internacoes(sk_diagnostico);
CREATE INDEX IF NOT EXISTS idx_fct_competencia  ON dwh.fct_internacoes(competencia);

-- Pré-popula dimensão diagnóstico com os CIDs alvo
INSERT INTO dwh.dim_diagnostico (cid, descricao, grupo, subcategoria) VALUES
    ('J45',  'Asma',                                          'Asma',       'CID J'),
    ('J450', 'Asma predominantemente alérgica',               'Asma',       'CID J'),
    ('J451', 'Asma não alérgica',                             'Asma',       'CID J'),
    ('J458', 'Asma mista',                                    'Asma',       'CID J'),
    ('J459', 'Asma não especificada',                         'Asma',       'CID J'),
    ('J30',  'Rinite alérgica e vasomotora',                  'Rinite',     'CID J'),
    ('J300', 'Rinite vasomotora',                             'Rinite',     'CID J'),
    ('J301', 'Rinite alérgica devida a pólen',                'Rinite',     'CID J'),
    ('J303', 'Outras rinites alérgicas',                      'Rinite',     'CID J'),
    ('J304', 'Rinite alérgica não especificada',              'Rinite',     'CID J'),
    ('L20',  'Dermatite atópica',                             'Dermatite',  'CID L'),
    ('L200', 'Liquenificação de Hebra',                       'Dermatite',  'CID L'),
    ('L208', 'Outras dermatites atópicas',                    'Dermatite',  'CID L'),
    ('L209', 'Dermatite atópica não especificada',            'Dermatite',  'CID L'),
    ('T78',  'Efeitos adversos NE e anafilaxia',              'Anafilaxia', 'CID T'),
    ('T780', 'Choque anafilático por alimento',               'Anafilaxia', 'CID T'),
    ('T781', 'Outras reações adversas a alimentos',           'Anafilaxia', 'CID T'),
    ('T782', 'Choque anafilático não especificado',           'Anafilaxia', 'CID T'),
    ('T784', 'Alergia não especificada',                      'Anafilaxia', 'CID T')
ON CONFLICT (cid) DO NOTHING;

-- Pré-popula dimensão tempo para 2025 (Jan-Dez)
INSERT INTO dwh.dim_tempo (competencia, ano, mes, trimestre, semestre, nome_mes)
VALUES
    ('202501', 2025, 1,  1, 1, 'Janeiro'),
    ('202502', 2025, 2,  1, 1, 'Fevereiro'),
    ('202503', 2025, 3,  1, 1, 'Março'),
    ('202504', 2025, 4,  2, 1, 'Abril'),
    ('202505', 2025, 5,  2, 1, 'Maio'),
    ('202506', 2025, 6,  2, 1, 'Junho'),
    ('202507', 2025, 7,  3, 2, 'Julho'),
    ('202508', 2025, 8,  3, 2, 'Agosto'),
    ('202509', 2025, 9,  3, 2, 'Setembro'),
    ('202510', 2025, 10, 4, 2, 'Outubro'),
    ('202511', 2025, 11, 4, 2, 'Novembro'),
    ('202512', 2025, 12, 4, 2, 'Dezembro')
ON CONFLICT (competencia) DO NOTHING;


-- ==============================================================
-- CAMADA GOLD (analytics) — KPIs e agregações
-- Estas tabelas são truncadas e recriadas a cada run da DAG.
-- ==============================================================

-- KPI 1: Volume e custo por município
CREATE TABLE IF NOT EXISTS analytics.kpi_municipio (
    cod_ibge            VARCHAR(7),
    nome_municipio      VARCHAR(100),
    uf                  VARCHAR(2),
    total_internacoes   INT,
    total_obitos        INT,
    taxa_mortalidade    NUMERIC(5,2),  -- %
    custo_total_rs      NUMERIC(14,2),
    custo_medio_rs      NUMERIC(10,2),
    media_dias_perm     NUMERIC(6,2),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (cod_ibge)
);

-- KPI 2: Volume e custo por diagnóstico (CID)
CREATE TABLE IF NOT EXISTS analytics.kpi_diagnostico (
    cid                 VARCHAR(5),
    descricao           VARCHAR(200),
    grupo               VARCHAR(100),
    total_internacoes   INT,
    total_obitos        INT,
    taxa_mortalidade    NUMERIC(5,2),
    custo_total_rs      NUMERIC(14,2),
    custo_medio_rs      NUMERIC(10,2),
    media_dias_perm     NUMERIC(6,2),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (cid)
);

-- KPI 3: Série temporal mensal
CREATE TABLE IF NOT EXISTS analytics.kpi_serie_temporal (
    competencia         VARCHAR(6),
    ano                 SMALLINT,
    mes                 SMALLINT,
    nome_mes            VARCHAR(20),
    total_internacoes   INT,
    total_obitos        INT,
    custo_total_rs      NUMERIC(14,2),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (competencia)
);

-- KPI 4: Perfil demográfico
CREATE TABLE IF NOT EXISTS analytics.kpi_demografico (
    grupo_etario        VARCHAR(30),  -- Ex: '0-9', '10-19', ...
    sexo                CHAR(1),
    total_internacoes   INT,
    total_obitos        INT,
    media_dias_perm     NUMERIC(6,2),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (grupo_etario, sexo)
);
