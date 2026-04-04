-- Camada Prata (DWH) - Limpeza e Modelagem dos Dados
-- Foco: Internações por Alergias e Asma (CIDs: J45, J30, L20, T78)

-- 1. Dimensão Diagnóstico
DROP TABLE IF EXISTS dwh.dim_diagnostico CASCADE;
CREATE TABLE dwh.dim_diagnostico AS
SELECT DISTINCT
    diag_princ AS id_cid,
    diag_princ AS codigo_cid
FROM raw.sih_rd
WHERE diag_princ SIMILAR TO '(J45|J30|L20|T78)%';

-- 2. Dimensão Localidade
DROP TABLE IF EXISTS dwh.dim_localidade CASCADE;
CREATE TABLE dwh.dim_localidade AS
SELECT DISTINCT
    munic_res AS id_municipio
FROM raw.sih_rd
WHERE diag_princ SIMILAR TO '(J45|J30|L20|T78)%';

-- 3. Dimensão Tempo
DROP TABLE IF EXISTS dwh.dim_tempo CASCADE;
CREATE TABLE dwh.dim_tempo AS
SELECT DISTINCT
    ano_cmpt AS ano,
    mes_cmpt AS mes
FROM raw.sih_rd
WHERE diag_princ SIMILAR TO '(J45|J30|L20|T78)%';

-- 4. Fato Internações por Alergias e Asma
DROP TABLE IF EXISTS dwh.fact_internacao_alergia CASCADE;
CREATE TABLE dwh.fact_internacao_alergia AS
SELECT
    n_aih AS id_internacao,
    diag_princ AS id_cid,
    munic_res AS id_municipio,
    ano_cmpt AS ano,
    mes_cmpt AS mes,
    sexo,
    idade,
    CAST(dias_perm AS INTEGER) AS dias_permanencia,
    CAST(val_tot AS NUMERIC(10,2)) AS valor_total_sus,
    CAST (morte AS INTEGER) AS flag_obito
FROM raw.sih_rd
WHERE diag_princ SIMILAR TO '(J45|J30|L20|T78)%';
