from airflow.decorators import dag, task
from datetime import datetime, timedelta
import json
import os
import pandas as pandas
from airflow.sensors.filesystem import FileSensor
from ftplib import FTP
#import py7zr
from os import walk


#path_folder= '/opt/airflow/dags/files/'
base = '/opt/airflow/dags/files/base/'
saida = '/opt/airflow/dags/files/extract/'

# Lista de Estados
estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'ESTB', 'GO', 
           'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 
           'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO' 
           ]

pastas2 = ['RAIS_ESTAB_PUB', 'RAIS_VINC_PUB_CENTRO_OESTE', 'RAIS_VINC_PUB_MG_ES_RJ',
           'RAIS_VINC_PUB_NORDESTE', 'RAIS_VINC_PUB_NORTE', 'RAIS_VINC_PUB_SP', 'RAIS_VINC_PUB_SUL'  
           ]


# Função pra checar se já existe o arquivo no diretório
def checadir(pasta, arquivo):
    files = []
    for (dirpath, dirnames, filenames) in walk(pasta):
        files.extend(filenames)
        break
    if arquivo in files:
        return True
    else:
        return False


default_args = {
    "owner": "Guma",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

@dag ("Input_datalake_A3data", default_args=default_args,schedule_interval="@daily",
    start_date=datetime(2021, 6 ,16),catchup=False)

def process_a3data():

    @task             
    def start_ ():                      ## start no processo
        print("Start !!!")

    @task               
    def download_base_ (retorno) :    # >>>>>>>>>>>>>>>>>  Fazendo o download dos arquivos no Ftp e salvando por ano
        ftp = FTP('ftp.mtps.gov.br')  ## SERVIDOR FTP
       
        for ano in range(2010,2020):
            arq= []
            lista=[]
            os.makedirs(base+str(ano), exist_ok=True)
            ftp.login()
            ftp.cwd("pdet/microdados/RAIS/"+str(ano))  ## acessando as pastas por ano
            ftp.dir(arq.append)
            lista = [(i.strip().split(' ')[-1]) for i in arq]  ## separando por espaço e pegando o ultimo elemento para nome do arquivo
            for i in lista:
                if not checadir(base+str(ano), i):
                    file = open(base+str(ano)+'/'+i, "wb") 
                    ftp.retrbinary("RETR " + i, file.write)         # escrevendo no arquivo
                    file.close()

    @task          
    def extrai_arq_2010_2017_(retorno):     ## >>>>>>>>>>>>>>>>>  Extraindo os arquivos da pasta base e salvando na pasta extract por estado
        for y in range(2010,2018):
            for x in estados:
                if not checadir(saida+str(y), x+str(y)+'.txt'):
                     with py7zr.SevenZipFile(base+str(y)+'/'+x+str(y)+'.7z', 'r') as archive:
                        archive.extractall(path=saida+str(y))          

    @task           
    def extrai_arq_2018_2019_(retorno) :    ## >>>>>>>>>>>>>>> # Extraindo os arquivos da pasta 2018 e 2019 por serem difentes dos outros anos
        for z in range(2018,2020):
            for k in pastas2:
                if not checadir(saida+str(z), k+'.txt'):
                    with py7zr.SevenZipFile(base+str(z)+'/'+k+'.7z', 'r') as archive:
                        archive.extractall(path=saida+str(z))    
            

 
    start = start_()
    download = download_base_(start)
    arq2010_2017= extrai_arq_2010_2017_(download)
    arq2018_2019= extrai_arq_2018_2019_(arq2010_2017)

    download >> arq2010_2017 >> arq2018_2019

exec = process_a3data()
