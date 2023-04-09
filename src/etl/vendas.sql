-- Databricks notebook source
WITH basePedidoItem AS (
  SELECT 
    item.*, 
    ped.dtPedido
  FROM silver.olist.pedido AS ped
  LEFT JOIN silver.olist.item_pedido AS item ON item.idPedido = ped.idPedido
  WHERE
    ped.dtPedido >= add_months('2018-01-01', -6) AND ped.dtPedido < '2018-01-01' AND
    item.idVendedor IS NOT NULL
),
basePedidos AS (
  SELECT DISTINCT
    idPedido,
    idVendedor,
    dtPedido,
    SUM(vlPreco) AS vl_TotalPedido,
    COUNT(DISTINCT idProduto) AS qtd_Produtos
  FROM basePedidoItem
  GROUP BY 1, 2, 3
), 
summary AS (
  SELECT
    item.idVendedor,
    
    COUNT(DISTINCT ped.idPedido) AS qtd_Pedidos,
    COUNT(DISTINCT ped.dtPedido) AS qtd_Dias,
    COUNT(item.idProduto) AS qtd_Produtos,
    COUNT(item.idProduto) / COUNT(DISTINCT ped.idPedido) AS avg_ProdutoPorPedido,
    COUNT(DISTINCT item.idProduto) AS qtd_ProdutosUnicos,
    date_diff('2018-01-01', MIN(ped.dtPedido)) AS qtd_DiasDesdePrimeiraVenda,
    date_diff('2018-01-01', MAX(ped.dtPedido)) AS qtd_Recencia,
    
    AVG(item.VlPreco) AS avg_VlPreco,
    PERCENTILE(item.VlPreco, 0.5) AS median_VlPreco,
    MIN(item.VlPreco) AS min_VlPreco,
    MAX(item.VlPreco) AS max_VlPreco,

    AVG(ped.vl_TotalPedido) AS avg_VlTotalPedido,
    PERCENTILE(ped.vl_TotalPedido, 0.5) AS median_VlTotalPedido,
    MAX(ped.vl_TotalPedido) AS max_VlTotalPedido,
    MIN(ped.vl_TotalPedido) AS min_VlTotalPedido,

    AVG(ped.qtd_Produtos) AS avg_QtdProdutosPorPedido,
    PERCENTILE(ped.qtd_Produtos, 0.5) AS median_QtdProdutosPorPedido,
    MAX(ped.qtd_Produtos) AS max_QtdProdutosPorPedido
  FROM basePedidos AS ped
  LEFT JOIN silver.olist.item_pedido AS item ON item.idPedido = ped.idPedido
  GROUP BY 1
), 
basePedidoSummary AS (
  SELECT
    idVendedor,
    idPedido,
    SUM(vlPreco) AS vlPreco
  FROM basePedidoItem
  GROUP BY 1, 2
),
baseLTV AS (
  SELECT
    item.idVendedor,
    SUM(item.vlPreco) AS LTV,
    MAX(date_diff('2018-01-01', dtPedido)) AS qtd_DiasBase
  FROM silver.olist.pedido AS ped
  LEFT JOIN silver.olist.item_pedido AS item ON item.idPedido = ped.idPedido
  WHERE
    ped.dtPedido < '2018-01-01' AND
    item.idVendedor IS NOT NULL
  GROUP BY 1
),
baseDatas AS (
  SELECT DISTINCT
    idVendedor,
    date(dtPedido)
  FROM basePedidoItem
  ORDER BY 1, 2
),
baseLag AS (
  SELECT *,
    LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1
  FROM baseDatas
),
baseIntervalo AS (
  SELECT
    idVendedor,
    AVG(datediff(dtPedido, lag1)) AS avg_IntervaloDias
  FROM baseLag
  GROUP BY 1
)
SELECT
  '2018-01-01' AS dtReference,
  t1.*,
  t3.LTV,
  t3.qtd_DiasBase,
  t4.avg_IntervaloDias
FROM summary AS t1
LEFT JOIN baseLTV AS t3 ON t3.idVendedor = t1.idVendedor
LEFT JOIN baseIntervalo AS t4 ON t4.idVendedor = t1.idVendedor
ORDER BY 1, 2;

-- COMMAND ----------

SELECT COUNT(*) FROM silver.olist.item_pedido
