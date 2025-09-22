
-- PROJETO FINAL EBAC — SQL (BigQuery)
-- Tema: Eventos do e‑commerce (Olist) — 2017
-- Autor(a): Bruna
-- Objetivo: criar um CSV único limpo e pronto para EDA/Looker, e consultas de negócio

-- As seções estão organizadas por blocos: origem, transformações, criação de colunas novas, e consultas de negócio.
-- Dataset: ecommerce-projeto-final.projeto_final

-- Bloco 1: ITENS DO PEDIDO + PRODUTO
CREATE OR REPLACE TABLE `ecommerce-projeto-final.projeto_final._tmp_itens_prod_2017` AS
WITH itens AS (
  SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    (price + freight_value) AS valor_item
  FROM `ecommerce-projeto-final.projeto_final.order_items`
)
SELECT
  i.*,
  p.product_category_name AS product_category_name_pt
FROM itens i
LEFT JOIN `ecommerce-projeto-final.projeto_final.products` p
  ON i.product_id = p.product_id
;

-- Bloco 2: ORDERS + CLASSIFICAÇÃO EVENTO
CREATE OR REPLACE TABLE `ecommerce-projeto-final.projeto_final._tmp_orders_evento_2017` AS
SELECT
  o.order_id,
  DATE(o.order_purchase_timestamp) AS order_date,
  CASE
    WHEN DATE(o.order_purchase_timestamp) BETWEEN DATE '2017-11-25' AND DATE '2017-12-25' THEN 'Natal'
    WHEN DATE(o.order_purchase_timestamp) BETWEEN DATE '2017-10-25' AND DATE '2017-11-24' THEN 'Black Friday'
    WHEN DATE(o.order_purchase_timestamp) BETWEEN DATE '2017-04-14' AND DATE '2017-05-14' THEN 'Dia das Mães'
    WHEN DATE(o.order_purchase_timestamp) BETWEEN DATE '2017-07-14' AND DATE '2017-08-13' THEN 'Dia dos Pais'
    WHEN DATE(o.order_purchase_timestamp) BETWEEN DATE '2017-02-13' AND DATE '2017-03-15' THEN 'Dia do Consumidor'
    ELSE NULL
  END AS evento_30d
FROM `ecommerce-projeto-final.projeto_final.orders` o
WHERE EXTRACT(YEAR FROM o.order_purchase_timestamp) = 2017
;

-- Bloco 3: VALOR DO PEDIDO + TICKET MÉDIO
CREATE OR REPLACE TABLE `ecommerce-projeto-final.projeto_final._tmp_pedido_evento_2017` AS
WITH soma_itens AS (
  SELECT
    order_id,
    SUM(valor_item) AS valor_pedido
  FROM `ecommerce-projeto-final.projeto_final._tmp_itens_prod_2017`
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.order_date,
  o.evento_30d,
  s.valor_pedido,
  AVG(s.valor_pedido) OVER (PARTITION BY o.evento_30d) AS ticket_medio
FROM `ecommerce-projeto-final.projeto_final._tmp_orders_evento_2017` o
JOIN soma_itens s
  ON o.order_id = s.order_id
WHERE o.evento_30d IS NOT NULL
;

-- Bloco 4: FORMA DE PAGAMENTO PREDOMINANTE
CREATE OR REPLACE TABLE `ecommerce-projeto-final.projeto_final._tmp_pagamento_predominante_2017` AS
SELECT
  order_id,
  payment_type AS tipo_pagamento
FROM (
  SELECT
    order_id,
    payment_type,
    payment_value,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY payment_value DESC, payment_type
    ) AS rk
  FROM `ecommerce-projeto-final.projeto_final.payments`
)
WHERE rk = 1
;

-- Bloco 5: PICO DE PEDIDOS
CREATE OR REPLACE TABLE `ecommerce-projeto-final.projeto_final._tmp_pico_evento_2017` AS
WITH diario AS (
  SELECT
    evento_30d,
    order_date,
    COUNT(DISTINCT order_id) AS pedidos
  FROM `ecommerce-projeto-final.projeto_final._tmp_orders_evento_2017`
  WHERE evento_30d IS NOT NULL
  GROUP BY evento_30d, order_date
)
SELECT
  evento_30d,
  order_date AS data_pico,
  pedidos AS volume_pico
FROM (
  SELECT
    d.*,
    ROW_NUMBER() OVER (PARTITION BY evento_30d ORDER BY pedidos DESC, order_date) AS rk
  FROM diario d
)
WHERE rk = 1
;

-- Bloco 6: TABELA FINAL
CREATE OR REPLACE TABLE `ecommerce-projeto-final.projeto_final.ecommerce_eventos_2017` AS
SELECT
  i.order_id,
  i.order_item_id,
  i.product_id,
  i.seller_id,
  o2.order_date,
  DATE(i.shipping_limit_date) AS shipping_limit_date,
  o.evento_30d,
  i.product_category_name_pt,
  i.price,
  i.freight_value,
  i.valor_item,
  o.valor_pedido,
  pp.tipo_pagamento,
  o.ticket_medio,
  pe.data_pico,
  pe.volume_pico
FROM `ecommerce-projeto-final.projeto_final._tmp_itens_prod_2017` i
JOIN `ecommerce-projeto-final.projeto_final._tmp_orders_evento_2017` o2
  ON i.order_id = o2.order_id
JOIN `ecommerce-projeto-final.projeto_final._tmp_pedido_evento_2017` o
  ON i.order_id = o.order_id
LEFT JOIN `ecommerce-projeto-final.projeto_final._tmp_pagamento_predominante_2017` pp
  ON i.order_id = pp.order_id
LEFT JOIN `ecommerce-projeto-final.projeto_final._tmp_pico_evento_2017` pe
  ON o.evento_30d = pe.evento_30d
WHERE o2.evento_30d IS NOT NULL
;
