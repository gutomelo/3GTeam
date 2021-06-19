# Databricks notebook source
# MAGIC %md
# MAGIC # Processo 4 - Upload da Extração para o DataLake

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fazer o download da biblioteca se necessário

# COMMAND ----------

!pip install azure-storage-file-datalake

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importando as bibliotecas

# COMMAND ----------

import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variáveis globais utilizadas no Azure

# COMMAND ----------

# Lista de diretórios
#base = './base/'
extract = '/dbfs/hackathon/extract/'
saida = 'hackathon/'

# Lista de Estados
estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'ESTB', 'GO', 
           'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 
           'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO' 
           ]

# Lista de Estados
pastas2 = ['RAIS_ESTAB_PUB', 'RAIS_VINC_PUB_CENTRO_OESTE', 'RAIS_VINC_PUB_MG_ES_RJ',
           'RAIS_VINC_PUB_NORDESTE', 'RAIS_VINC_PUB_NORTE', 'RAIS_VINC_PUB_SP', 'RAIS_VINC_PUB_SUL'  
           ]


meudatalake = 'datalakehackathon'
meucontainer = 'dadosbrutos'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando o serviço de conexão com o DataLake

# COMMAND ----------

def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inicializando a conexão com o Datalake

# COMMAND ----------

initialize_storage_account(meudatalake, 
'ew3BTntHyyOiArGLqGv8OC3MyHADZGmvowh4Ir9/D4TrJfs58WBXsvZ13reC535Q7o2od15gC4Wd0SEeQsHLCQ==')

# COMMAND ----------

# Função para checar se existe o arquivo no diretorio

def checadlake(diretorio, arquivo):
    try:
        dirlist = []        
        file_system_client = service_client.get_file_system_client(file_system=meucontainer)
        paths = file_system_client.get_paths(path=diretorio)
        
        for path in paths:
            dirlist.append(path.name)
        
        if diretorio+arquivo in dirlist:
            print(f'O arquivo {arquivo} já existe no Datalake! Indo para o próximo!\n')
            return True
        else:
            return False

    except Exception as e:
        pass
        

# COMMAND ----------

checadlake('gutodir/', 'Teste01.ipynb')

# COMMAND ----------

checadlake('', 'A3_data.ipynb')

# COMMAND ----------

checadlake('extract/2010/', 'AC2010.txt')

# COMMAND ----------

def upload_big(diretorio, arquivo):
    try:

        file_system_client = service_client.get_file_system_client(file_system=meucontainer)

        directory_client = file_system_client.get_directory_client(diretorio)
        
        file_client = directory_client.get_file_client(arquivo)

        local_file = open(diretorio+arquivo,'rb')

        file_contents = local_file.read()

        file_client.upload_data(file_contents, overwrite=True)

    except Exception as e:
      print(e)

# COMMAND ----------

# Upload das pastas de 2010 a 2017 para o Datalake
for y in range(2010,2012):
    for x in estados:
        if not checadlake(saida+str(y)+'/', x+str(y)+'.txt'):
            upload_big(extract+str(y)+'/', x+str(y)+'.txt')
            print(f'O arquivo {x+str(y)}.txt foi enviado para o DataLake!')

# COMMAND ----------



# COMMAND ----------

