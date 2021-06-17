# OBS: !!!!Projeto em construção!!!


## O projeto consiste em fazer o download do arquivos Rais no ftp do Ministério do trabalho. Descompactar os arquivos que estão no formato 7-Zip. E enviar para o Datalake no Azure Datalake Gen2.

## Todo processo será orquestrado pelo Apache Airflow.


## Procedimentos:
O AirFlow será executado num container Docker dentro de uma máquina virtual com o a distribuição Ubuntu no Azure.

Aṕos criado a máquina virtual com o Ubuntu e configurado o docker e docker-compose, fazer o download projeto Yaml no site do Apache Airflow.

Comandos:

> curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.1.0/docker-compose.yaml'

> docker-compose up -d


 
