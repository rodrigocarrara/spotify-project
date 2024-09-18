

Descrição: Projeto de dados em construção utilizando a base Spotify Charts disponibilizada no Kaggle (link: [Spotify Charts](https://www.kaggle.com/datasets/sunnykakar/spotify-charts-all-audio-data)).

Etapas realizadas:

1. Extração da base da API e pré-tratamento dos dados no diretório local.
2. Base pré-tratada exportada para o Data Lake do Azure na camada inbound.
3. Processamento dos dados através do Databricks do Azure para as camadas bronze e silver no formato de delta table.
4. Orquestração de pipelines através do Data Factory.

