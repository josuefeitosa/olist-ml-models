-- Databricks notebook source
WITH base AS (
  SELECT DISTINCT
    ped.idPedido,
    item.idVendedor,
    aval.vlNota
  FROM silver.olist.pedido AS ped
  LEFT JOIN silver.olist.item_pedido AS item ON item.idPedido = ped.idPedido
  LEFT JOIN silver.olist.avaliacao_pedido aval ON aval.idPedido = ped.idPedido
), summaryBase AS (
  SELECT
    idVendedor,
    COUNT(vlNota) / COUNT(DISTINCT idPedido) AS per_Avaliacoes,
    AVG(vlNota) AS avg_Nota,
    PERCENTILE(vlNota, 0.5) AS median_Nota,
    MIN(vlNota) AS min_Nota,
    MAX(vlNota) AS max_Nota
  FROM base
  GROUP BY 1
)
SELECT
  '2018-01-01' AS dtReference,
  *
FROM summaryBase

