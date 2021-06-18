# Databricks notebook source
# MAGIC %md
# MAGIC # Processo 1 - Download FTP

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importando as bibliotecas

# COMMAND ----------

import os
from os import walk
from ftplib import FTP

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nome do diretório para armazenamento

# COMMAND ----------

base = '/dbfs/hackathon/base/'

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
# MAGIC ### Criando conexão FTP

# COMMAND ----------

ftp = FTP('ftp.mtps.gov.br')  ## SERVIDOR FTP

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fazendo o download dos arquivos no Ftp e salvando por ano

# COMMAND ----------

for ano in range(2010,2020):
    arq= []
    lista=[]
    os.makedirs(base+str(ano), exist_ok=True)
    ftp.login()
    ftp.cwd("pdet/microdados/RAIS/"+str(ano))          ## acessando as pastas por ano
    ftp.dir(arq.append)
    lista = [(i.strip().split(' ')[-1]) for i in arq]  ## separando por espaço e pegando o ultimo elemento para nome do arquivo
    for i in lista:
        if not checadir(base+str(ano), i):
            file = open(base+str(ano)+'/'+i, "wb") 
            ftp.retrbinary("RETR " + i, file.write)    # escrevendo no arquivo
            file.close()
            print(f'Feito o Download do arquivo: {i}\n')
print('>>> Todos os Downloads foram realizados com sucesso! <<<')            