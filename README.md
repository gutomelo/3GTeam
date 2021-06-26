## Pipeline  de extração, transformação e disponibilização dos dados !

Este repositório representa o projeto apresentado pela equipe 3GTeam no Hackathon da A3Data no mês de Junho de 2021.

Onde os times devem implementar pipeline de extração, transformação e disponibilização de dados. Após extração, limpeza, organização e estruturação  dos dados, as perguntas chave do desafio devem ser respondidas de maneira visual. 

Perguntas Chave:

1. Nos últimos 10 anos, quais foram os salários médios de homens e mulheres que trabalham com tecnologia na região sudeste do Brasil por ano?
2. Nos últimos 10 anos, quais foram os salários médios das pessoas por nível de escolaridade que trabalhavam no setor de agronegócio na região sul do Brasil?
3. Entre os setores de tecnologia, indústria automobilística e profissionais da saúde, qual deles teve o maior crescimento? Qual foi o número de trabalhadores em cada setor por ano?
4. Nos últimos 10 anos, quais foram os setores que possuem, em cada ano, o maior número de trabalhadores que possuem jornada semanal inferior a 40h?
5. Qual é o número absoluto de pessoas por cada categoria de sexo que realizaram trabalho intermitente em cada um dos últimos anos?

### Proposta

O projeto consiste na construção de um Pipeline de ETL dos dados abertos da RAIS disponibilizados pelo governo em um servidor FTP. (ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/) do Ministério do Trabalho. Todo o pipeline foi contruido em cima do Azure DataFactory de forma totalmente automatizada.


## Procedimentos

### Arquitetura

![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/arquitetura.png?raw=true)





### As tecnologias trabalhadas nesse projeto são baseadas na Cloud da Azure:



- Data Factory - Orquestração do Pipeline (Notebooks)

  ![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/pipeline1.png?raw=true)

  



- Databricks - Processamento  em Spark do download, extração, limpeza  e tratamento dos dados

  ![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/databricks.png?raw=true)





- Data Lake Storage Gen2 - Armazenamento dos dados txt e parquet

  ![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/datalake all.png?raw=true)





- Synapse - Criação de tableas externas referenciando o local dos dados no data lake

  ![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/tabela_externa_synapse.JPG?raw=true)





- Power BI - Visualização dos dados

  ![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/trabalho_intermitente.PNG?raw=true)

  ![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/agronegocio.PNG?raw=true)

  

  ![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/salario meio tecnologia.PNG?raw=true)

  

### Etapas realizadas:

- Extração dos arquivos da RAIS de um servidor FTP do Ministério do Trabalho
- Descompactação dos arquivos 7Z para txt
- Processamento das bases, limpeza e tratamento dos dados e criação de tabelas no Databricks
- Armazenamento da base completa em formato Parquet no data lake
- Utilização do Azure Synapase para criação de tabelas externas referenciando o local dos arquivos parquet no data lake
- Power BI para visualizção dos dados no Synapse

### Estrutura do datalake:

![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/estrutura_datalakepng.png?raw=true)



### Custo Total aproximado do projeto

![alt text](https://github.com/gutomelo/3GTeam/blob/master/images/Custo Total.png?raw=true)





