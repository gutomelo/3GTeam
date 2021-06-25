

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '190621@Data'; 

 -- 2 -CRIANDO SCOPO DO BD PARA AUTH NO ARMAZENAMENTO AZURE

CREATE DATABASE SCOPED CREDENTIAL CredencialAzureProjeto2
WITH IDENTITY = 'datalakehackathon' , --name storage account
SECRET = 'ew3BTntHykkkkOCkkkkkkkkkkkkkkkkkkkkkkreC535Q7o2odkkkkkkkeQsHLCQ==';

-- 3 -- CRIANDO UMA FONTE DE DADOS EXTERNA

CREATE EXTERNAL DATA SOURCE DataLakeA3v2
WITH
	(
	TYPE = Hadoop,
	LOCATION = 'abfss://refined@accountstorage.dfs.core.windows.net',
	CREDENTIAL = CredencialAzureProjeto2
	) ;

-- 4 - INFORMANDO FORMATO DOS DADOS ARMAZENADOS

CREATE EXTERNAL FILE FORMAT formatotext
WITH 
	(
	FORMAT_TYPE= PARQUET,
	DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
	);


-- 5 - CRIANDO TABELA EXTERNA REFERENCIANDO O ARQUIVO NO DATALAKE

CREATE EXTERNAL TABLE tbl_testea3 (
Ano INT,
Sexo_Trabalhador VARCHAR(50),
Idade INT,
Escolaridade_apos_2005 INT,
CNAE_2_0_Classe VARCHAR(50),
CNAE_2_0_Subclasse VARCHAR(50),
Tipo_Vinculo VARCHAR(50),
Qtd_Hora_Contr INT,
Ind_Trab_intermitente INT,
Vl_Remun_Media_Nom FLOAT,
UF  CHAR(2)
)
WITH (
	LOCATION= '/rais/',
	DATA_SOURCE= DataLakeA3v2,
	FILE_FORMAT= formatotext
	);










SELECT ano  FROM tbl_testea3 ;


-- colunas que possivelmente vai no arquivo final

