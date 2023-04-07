-- Databricks notebook source
WITH base AS (
  SELECT DISTINCT
    item.idVendedor,
    prod.*
  FROM silver.olist.pedido AS ped
  LEFT JOIN silver.olist.item_pedido AS item ON ped.idPedido = item.idPedido
  LEFT JOIN silver.olist.produto AS prod ON prod.idProduto = item.idProduto
  WHERE
    ped.dtPedido >= add_months('2018-01-01', -6) AND ped.dtPedido < '2018-01-01' AND
    item.idVendedor IS NOT NULL
), top15_Categorias AS (
  SELECT
    prod.descCategoria
    FROM silver.olist.pedido AS ped
    LEFT JOIN silver.olist.item_pedido AS item ON ped.idPedido = item.idPedido
    LEFT JOIN silver.olist.produto AS prod ON prod.idProduto = item.idProduto
  GROUP BY 1
  ORDER BY COUNT(DISTINCT ped.idPedido) DESC
  LIMIT 15
), summaryBase AS (
  SELECT
    idVendedor,
    COUNT(DISTINCT idProduto) AS qtd_Produto,

    AVG(COALESCE(nrFotos, 0)) AS avg_NrFotos,

    AVG(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS avg_VolumeProduto,
    PERCENTILE(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) AS median_VolumeProduto,
    MIN(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS min_VolumeProduto,
    MAX(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS max_VolumeNrFotoProdutos,

    COUNT(DISTINCT CASE WHEN descCategoria IN (SELECT * FROM top15_Categorias) THEN descCategoria END) AS qtd_CategoriaTop15,

    COUNT(DISTINCT CASE WHEN descCategoria = 'cama_mesa_banho' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_cama_mesa_banho,
    COUNT(DISTINCT CASE WHEN descCategoria = 'beleza_saude' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_beleza_saude,
    COUNT(DISTINCT CASE WHEN descCategoria = 'esporte_lazer' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_esporte_lazer,
    COUNT(DISTINCT CASE WHEN descCategoria = 'informatica_acessorios' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_informatica_acessorios,
    COUNT(DISTINCT CASE WHEN descCategoria = 'moveis_decoracao' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_moveis_decoracao,
    COUNT(DISTINCT CASE WHEN descCategoria = 'utilidades_domesticas' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_utilidades_domesticas,
    COUNT(DISTINCT CASE WHEN descCategoria = 'relogios_presentes' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_relogios_presentes,
    COUNT(DISTINCT CASE WHEN descCategoria = 'telefonia' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_telefonia,
    COUNT(DISTINCT CASE WHEN descCategoria = 'automotivo' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_automotivo,
    COUNT(DISTINCT CASE WHEN descCategoria = 'brinquedos' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_brinquedos,
    COUNT(DISTINCT CASE WHEN descCategoria = 'cool_stuff' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_cool_stuff,
    COUNT(DISTINCT CASE WHEN descCategoria = 'ferramentas_jardim' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_ferramentas_jardim,
    COUNT(DISTINCT CASE WHEN descCategoria = 'perfumaria' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_perfumaria,
    COUNT(DISTINCT CASE WHEN descCategoria = 'bebes' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_bebes,
    COUNT(DISTINCT CASE WHEN descCategoria = 'eletronicos' THEN idProduto END) / COUNT(DISTINCT idProduto) AS per_Categoria_eletronicos
  FROM base
  GROUP BY 1
)
SELECT 
  '2018-01-01' AS dtReference,
  *
FROM summaryBase
