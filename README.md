## Pipeline  de extração, transformação e disponibilização dos dados !

[image](https://user-images.githubusercontent.com/67971755/122650525-e0b80800-d109-11eb-997b-4009720ebae0.png)


Proposta: O projeto consiste em fazer o download do arquivos Rais no ftp do Ministério do trabalho,
Descompactar os arquivos que estão no formato 7-Zip. E enviar para o Datalake no Azure Datalake Gen2.

Nossa arqutetura é baseado na Cloud da Azure, utilizamos o Databricks na produção do código PYthon e processamento dos dados com o Spark.
Todo o pipeline foi contruido em cima do Azure DataFactory de forma totalmente automatizada.

## Procedimentos:
-  Utilizado o DataBricks e a biblioteca FTP para download dos dados do servidor FTP
(ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/)

- Utilizado  o DataBricks para extração e processamento dos dados para arquivos Parquet (não tratados)

- Extraidos os dados no Datalake no formato Parquet (Tratados)

- Utilizado o Azure Synapse para criação de tabelas externas visualizando os dados
do Data Lake Storage.

- Utilizado o Power Bi para visualização dos dados, tanto da base geral no Synapse,
quanto as tables no DataBricks.
