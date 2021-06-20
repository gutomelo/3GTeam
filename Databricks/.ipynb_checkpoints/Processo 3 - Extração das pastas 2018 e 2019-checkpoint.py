# Databricks notebook source
# MAGIC %md
# MAGIC # Processo 3 - Extração das pastas 2018 e 2019

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baixar biblioteca se necessário

# COMMAND ----------

!pip install py7zr

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importação da biblioteca para extração dos arquivos 7Zip

# COMMAND ----------

import py7zr
from os import walk

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lista de variáveis globais utilizada no código

# COMMAND ----------

# Lista de diretórios
base = '/dbfs/hackathon/base/'
saida = '/dbfs/hackathon/extract/'


# Lista de Estados
pastas2 = ['RAIS_ESTAB_PUB', 'RAIS_VINC_PUB_CENTRO_OESTE', 'RAIS_VINC_PUB_MG_ES_RJ',
           'RAIS_VINC_PUB_NORDESTE', 'RAIS_VINC_PUB_NORTE', 'RAIS_VINC_PUB_SP', 'RAIS_VINC_PUB_SUL'  
           ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Função pra checar se já existe o arquivo no diretório

# COMMAND ----------

def checadir(pasta, arquivo):
    files = []
    for (dirpath, dirnames, filenames) in walk(pasta):
        files.extend(filenames)
        break
    if arquivo in files:
        print(f'O arquivo {arquivo} já existe. Indo para o próximo!\n')
        return True
    else:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraindo os arquivos da pasta base e salvando na pasta extract por estado

# COMMAND ----------

# Extraindo as pastas de 2017 e 2018
for z in range(2018,2020):
    for k in pastas2:
        if not checadir(saida+str(z), k+'.txt'):
            with py7zr.SevenZipFile(base+str(z)+'/'+k+'.7z', 'r') as archive:
                archive.extractall(path=saida+str(z))
                print(f'O arquivo {k+str(z)}.txt foi extraído!\n')
print('>>> A extração das pastas de 2018 e 2018 foram realizadas com sucesso! <<<')