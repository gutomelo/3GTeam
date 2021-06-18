# Databricks notebook source
# MAGIC %md
# MAGIC # Processo 2 - Extração das pastas de 2010 a 2017

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baixar a biblioteca se necessário

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
estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'ESTB', 'GO', 
           'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 
           'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO' 
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

# Extraindo pastas de 2010 a 2017
for y in range(2010,2018):
    for x in estados:
        if not checadir(saida+str(y), x+str(y)+'.txt'):
            with py7zr.SevenZipFile(base+str(y)+'/'+x+str(y)+'.7z', 'r') as archive:
                archive.extractall(path=saida+str(y))
                print(f'O arquivo {x+str(y)}.txt foi extraído!\n')
print('>>> A extração das pastas de 2010 a 2017 foram realizadas com sucesso! <<<')