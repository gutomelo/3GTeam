## Pipeline  de extração, transformação e disponibilização dos dados !

### Proposta

O projeto consiste na construção de um Pipeline de ETL dos dados abertos da RAIS disponibilizados pelo governo em um [servidor FTP](ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/) do Ministério do Trabalho. Todo o pipeline foi contruido em cima do Azure DataFactory de forma totalmente automatizada.


## Procedimentos

# Arquitetura

[alt text](https://github.com/gutomelo/3GTeam/blob/master/Png/arquitetura.png)

### As tecnologias trabalhadas nesse projeto são baseadas na Cloud da Azure:

- Data Factory - Orquestração do Pipeline (Notebooks)
- Databricks - Processamento  em Spark do download, extração, limpeza  e tratamento dos dados
- Data Lake Storage Gen2 - Armazenamento dos dados txt e parquet
- Synapse - Criação de tableas externas referenciando o local dos dados no data lake
- Power BI - Visualização dos dados

### Etapas realizadas:

- Extração dos arquivos da RAIS de um [servidor FTP](ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/) do Ministério do Trabalho
- Descompactação dos arquivos 7Z para txt
- Processamento das bases, limpeza e tratamento dos dados e criação de tabelas no Databricks
- Armazenamento da base completa em formato Parquet no data lake
- Utilização do Azure Synapase para criação de tabelas externas referenciando o local dos arquivos parquet no data lake
- Power BI para visualizção dos dados no Synapse

### Estrutura do datalake:

[alt text](https://github.com/gutomelo/3GTeam/blob/master/Png/estrutura_datalakepng.png)

