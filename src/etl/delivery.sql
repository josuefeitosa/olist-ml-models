-- Databricks notebook source
WITH base AS (
  SELECT DISTINCT
    ped.idPedido,
    item.idVendedor,
    ped.descSituacao,
    ped.dtPedido,
    ped.dtAprovado,
    ped.dtEnvio,
    ped.dtEntregue,
    ped.dtEstimativaEntrega,
    SUM(item.vlFrete) as vlFrete
  FROM silver.olist.pedido as ped
  LEFT JOIN silver.olist.item_pedido as item ON item.idPedido = ped.idPedido
  WHERE
    ped.dtPedido >= add_months('2018-01-01', -6) AND ped.dtPedido < '2018-01-01' AND
    item.idVendedor IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT
  '2018-01-01' AS dtReference,
  idVendedor,

  COUNT(DISTINCT CASE WHEN date(coalesce(date(dtEntregue), '218-01-01')) > date(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS per_EntregaEmAtraso,

  COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_PedidosCancelados,

  AVG(vlFrete) AS avg_ValorFrete,
  PERCENTILE(vlFrete, 0.5) AS median_ValorFrete,
  MIN(vlFrete) AS min_ValorFrete,
  MAX(vlFrete) AS max_ValorFrete,

  AVG(date_diff(coalesce(date(dtEntregue), '2018-01-01'), date(dtPedido))) AS avg_DiasPedidoEntrega,
  PERCENTILE(date_diff(coalesce(date(dtEntregue), '2018-01-01'), date(dtPedido)), 0.5) AS median_DiasPedidoEntrega,
  MIN(date_diff(coalesce(date(dtEntregue), '2018-01-01'), date(dtPedido))) AS min_DiasPedidoEntrega,
  MAX(date_diff(coalesce(date(dtEntregue), '2018-01-01'), date(dtPedido))) AS max_DiasPedidoEntrega,

  AVG(date_diff(coalesce(date(dtEntregue), '2018-01-01'), date(dtAprovado))) AS avg_DiasAprovacaoEntrega,
  PERCENTILE(date_diff(coalesce(date(dtEntregue), '2018-01-01'), date(dtAprovado)), 0.5) AS median_DiasAprovacaoEntrega,
  MIN(date_diff(coalesce(date(dtEntregue), '2018-01-01'), date(dtAprovado))) AS min_DiasAprovacaoEntrega,
  MAX(date_diff(coalesce(date(dtEntregue), '2018-01-01'), date(dtAprovado))) AS max_DiasAprovacaoEntregaDiasAprovacaoEntrega,

  AVG(date_diff(date(dtEstimativaEntrega), coalesce(date(dtEntregue), '2018-01-01'))) AS avg_EntregaPromessa,
  PERCENTILE(date_diff(date(dtEstimativaEntrega), coalesce(date(dtEntregue), '2018-01-01')), 0.5) AS median_EntregaPromessa,
  MIN(date_diff(date(dtEstimativaEntrega), coalesce(date(dtEntregue), '2018-01-01'))) AS min_EntregaPromessa,
  MAX(date_diff(date(dtEstimativaEntrega), coalesce(date(dtEntregue), '2018-01-01'))) AS max_EntregaPromessa
FROM base
GROUP BY 1, 2
