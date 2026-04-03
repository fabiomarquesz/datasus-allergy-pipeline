-- ==============================================================================
-- CAMADA OURO (ANALYTICS) - Agregações Finais e KPIs
-- ==============================================================================

-- 1. Resumo por Doença
DROP TABLE IF EXISTS analytics.resumo_por_doenca CASCADE;
CREATE TABLE analytics.resumo_por_doenca AS
SELECT 
    d.codigo_cid,
    COUNT(f.id_internacao) AS total_internacoes,
    SUM(f.valor_total_sus) AS custo_total_reais,
    ROUND(AVG(f.dias_permanencia), 1) AS media_dias_internacao,
    SUM(f.flag_obito) AS total_obitos
FROM dwh.fact_internacao_alergia f
JOIN dwh.dim_diagnostico d ON f.id_cid = d.id_cid
GROUP BY d.codigo_cid
ORDER BY total_internacoes DESC;

-- 2. Resumo por Município
DROP TABLE IF EXISTS analytics.resumo_por_municipio CASCADE;
CREATE TABLE analytics.resumo_por_municipio AS
SELECT 
    l.id_municipio,
    COUNT(f.id_internacao) AS total_internacoes,
    SUM(f.valor_total_sus) AS custo_total_reais
FROM dwh.fact_internacao_alergia f
JOIN dwh.dim_localidade l ON f.id_municipio = l.id_municipio
GROUP BY l.id_municipio
ORDER BY total_internacoes DESC;