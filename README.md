# OBS: !!!!Projeto em construção!!!


## O projeto consiste em fazer o download do arquivos Rais no ftp do Ministério do trabalho. Descompactar os arquivos que estão no formato 7-Zip. E enviar para o Datalake no Azure Datalake Gen2.

## Todo processo será orquestrado pelo Apache Airflow.


## Procedimentos:
-  Utilizado o DataBricks e a biblioteca FTP para download dos dados do servidor FTP
(ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/)

- Utilizado  o DataBricks para extração e processamento dos dados para arquivos Parquet (não tratados)

- Extraidos os dados no Datalake no formato Parquet (Tratados)

- Utilizado o Azure Synapse para criação de tabelas externas visualizando os dados
do Data Lake Storage.

- Utilizado o Power Bi para visualização dos dados, tanto da base geral no Synapse,
quanto as tables no DataBricks.
