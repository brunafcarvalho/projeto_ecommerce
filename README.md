# Projeto Final EBAC — Análise de Eventos no E-commerce

Este projeto faz parte do curso **Profissão: Analista de Dados** da EBAC e tem como objetivo aplicar as principais etapas de um processo de análise de dados com foco em comportamento de consumo em datas comemorativas.

---

## 🎯 Objetivo

Analisar o comportamento de compras no e-commerce brasileiro (base Olist) durante as seguintes datas comemorativas de 2017:

- Black Friday
- Natal
- Dia das Mães
- Dia dos Pais
- Dia do Consumidor

---

## 🧰 Ferramentas utilizadas

- Google BigQuery (SQL)
- Google Colab (Python)
- Pandas, Matplotlib, Seaborn
- Looker Studio (Dashboard)
- GitHub (versionamento e entrega final)

---

## 🧠 Perguntas respondidas

1. Qual data comemorativa teve maior volume de pedidos?
2. Qual evento gerou maior faturamento?
3. Qual o ticket médio por evento e forma de pagamento?
4. Qual a forma de pagamento predominante em cada evento?
5. Quais foram as categorias mais vendidas (Top 5) por evento?
6. Qual o dia de maior pico de compras em cada data?

---

## 📊 Principais insights

- O evento com **maior volume de pedidos** foi o **Natal** (~7.100 pedidos)
- O **maior ticket médio** foi registrado em **Dia das Mães (boleto)**: R$ 203,08
- A forma de pagamento mais usada foi **cartão de crédito**
- A categoria **cama_mesa_banho** foi a mais presente nos eventos analisados
- A **Black Friday** teve um pico extremamente concentrado no dia 24/11/2017
- O **Natal** teve um volume mais distribuído ao longo do mês

---

## 💾 Acesso ao arquivo CSV final

📂 O arquivo final contendo todas as colunas tratadas, cruzadas e agregadas está disponível no link abaixo:

🔗 [Download do CSV Final (Google Drive)](https://drive.google.com/file/d/1_pGQWZGsZbuF9UrKgRifHfK2a9GoaC83/view?usp=sharing)

---

## 📈 Dashboard Interativo

🔧 (Em construção)  
Assim que finalizado no Looker Studio, o link será disponibilizado aqui.

---

## 📌 Como reproduzir

1. Acesse o notebook na pasta `eda/`
2. Baixe o CSV final ou monte o Drive no Colab
3. Execute as células para visualizar gráficos e análises
4. As queries SQL podem ser acessadas em `sql/consultas_bigquery.sql`

---

## 📚 Fonte de dados

- [Olist e-commerce dataset - Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

Projeto desenvolvido por **Bruna Carvalho**  
LinkedIn: [brunafmcarvalho](https://www.linkedin.com/in/brunafmcarvalho/)
