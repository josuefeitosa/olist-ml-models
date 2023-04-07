-- Databricks notebook source
WITH base AS (
  SELECT DISTINCT
    ped.idPedido,
    ped.idCliente,
    item.idVendedor,
    cli.descUF
  FROM silver.olist.pedido as ped
  LEFT JOIN silver.olist.item_pedido as item ON item.idPedido = ped.idPedido
  LEFT JOIN silver.olist.cliente as cli ON cli.idCliente = ped.idCliente
  WHERE
    ped.dtPedido >= add_months('2018-01-01', -6) AND ped.dtPedido < '2018-01-01' AND
    item.idVendedor IS NOT NULL 
), summaryBase AS (
  SELECT
    idVendedor,
    COUNT(DISTINCT descUF) AS qtd_UF,
    COUNT(DISTINCT CASE WHEN descUF = 'SC' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_SC,
    COUNT(DISTINCT CASE WHEN descUF = 'RO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_RO,
    COUNT(DISTINCT CASE WHEN descUF = 'PI' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_PI,
    COUNT(DISTINCT CASE WHEN descUF = 'AM' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_AM,
    COUNT(DISTINCT CASE WHEN descUF = 'RR' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_RR,
    COUNT(DISTINCT CASE WHEN descUF = 'GO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_GO,
    COUNT(DISTINCT CASE WHEN descUF = 'TO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_TO,
    COUNT(DISTINCT CASE WHEN descUF = 'MT' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_MT,
    COUNT(DISTINCT CASE WHEN descUF = 'SP' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_SP,
    COUNT(DISTINCT CASE WHEN descUF = 'ES' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_ES,
    COUNT(DISTINCT CASE WHEN descUF = 'PB' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_PB,
    COUNT(DISTINCT CASE WHEN descUF = 'RS' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_RS,
    COUNT(DISTINCT CASE WHEN descUF = 'MS' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_MS,
    COUNT(DISTINCT CASE WHEN descUF = 'AL' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_AL,
    COUNT(DISTINCT CASE WHEN descUF = 'MG' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_MG,
    COUNT(DISTINCT CASE WHEN descUF = 'PA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_PA,
    COUNT(DISTINCT CASE WHEN descUF = 'BA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_BA,
    COUNT(DISTINCT CASE WHEN descUF = 'SE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_SE,
    COUNT(DISTINCT CASE WHEN descUF = 'PE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_PE,
    COUNT(DISTINCT CASE WHEN descUF = 'CE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_CE,
    COUNT(DISTINCT CASE WHEN descUF = 'RN' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_RN,
    COUNT(DISTINCT CASE WHEN descUF = 'RJ' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_RJ,
    COUNT(DISTINCT CASE WHEN descUF = 'MA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_MA,
    COUNT(DISTINCT CASE WHEN descUF = 'AC' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_AC,
    COUNT(DISTINCT CASE WHEN descUF = 'DF' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_DF,
    COUNT(DISTINCT CASE WHEN descUF = 'PR' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_PR,
    COUNT(DISTINCT CASE WHEN descUF = 'AP' THEN idPedido END) / COUNT(DISTINCT idPedido) AS per_AP
  FROM base
GROUP BY 1
)
SELECT
  '2018-01-01' AS dtReference,
  *
FROM summaryBase
