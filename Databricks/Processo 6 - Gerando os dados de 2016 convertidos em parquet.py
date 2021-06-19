# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit

import pandas as pd
import databricks.koalas as ks

# COMMAND ----------

# Lista de diretórios
base = '/hackathon/extract/'
saida = '/mnt/trusted/'
schema = '/dbfs/schema'

# COMMAND ----------

# Create Spark Session

spark = SparkSession.builder.appName("Spark").getOrCreate()

# COMMAND ----------

# Schema 2016

schema_2016 =   StructType([
					 StructField("Bairros_SP", StringType(), True),
					 StructField("Bairros_Fortaleza", StringType(), True),
					 StructField("Bairros_RJ", StringType(), True),
					 StructField("Causa_Afastamento_1", StringType(), True),
					 StructField("Causa_Afastamento_2", StringType(), True),
					 StructField("Causa_Afastamento_3", StringType(), True),
					 StructField("Motivo_Desligamento", StringType(), True),
					 StructField("CBO_Ocupacao_2002", StringType(), True),
					 StructField("CNAE_2_0_Classe", StringType(), True),
					 StructField("CNAE_95_Classe", StringType(), True),
					 StructField("Distritos_SP", StringType(), True),
					 StructField("Vinculo_Ativo_31_12", StringType(), True),
					 StructField("Faixa_Etaria", StringType(), True),
					 StructField("Faixa_Hora_Contrat", StringType(), True),
					 StructField("Faixa_Remun_Dezem_SM", StringType(), True),
					 StructField("Faixa_Remun_Media_SM", StringType(), True),
					 StructField("Faixa_Tempo_Emprego", StringType(), True),
					 StructField("Escolaridade_apos_2005", StringType(), True),
					 StructField("Qtd_Hora_Contr", StringType(), True),
					 StructField("Idade", StringType(), True),
					 StructField("Ind_CEI_Vinculado", StringType(), True),
					 StructField("Ind_Simples", StringType(), True),
					 StructField("Mes_Admissao", StringType(), True),
					 StructField("Mes_Desligamento", StringType(), True),
					 StructField("Mun_Trab", StringType(), True),
					 StructField("Municipio", StringType(), True),
					 StructField("Nacionalidade", StringType(), True),
					 StructField("Natureza_Juridica", StringType(), True),
					 StructField("Ind_Portador_Defic", StringType(), True),
					 StructField("Qtd_Dias_Afastamento", StringType(), True),
					 StructField("Raca_Cor", StringType(), True),
					 StructField("Regioes_Adm_DF", StringType(), True),
					 StructField("Vl_Remun_Dezembro_Nom", StringType(), True),
					 StructField("Vl_Remun_Dezembro_SM", StringType(), True),
					 StructField("Vl_Remun_Media_Nom", StringType(), True),
					 StructField("Vl_Remun_Media_SM", StringType(), True),
					 StructField("CNAE_2_0_Subclasse", StringType(), True),
					 StructField("Sexo_Trabalhador", StringType(), True),
					 StructField("Tamanho_Estabelecimento", StringType(), True),
					 StructField("Tempo_Emprego", StringType(), True),
					 StructField("Tipo_Admissao", StringType(), True),
					 StructField("Tipo_Estab", StringType(), True),
					 StructField("Tipo_Estab_1", StringType(), True),
					 StructField("Tipo_Defic", StringType(), True),
					 StructField("Tipo_Vinculo", StringType(), True),
					 StructField("IBGE_Subsetor", StringType(), True),
					 StructField("Vl_Rem_Janeiro_CC", StringType(), True),
					 StructField("Vl_Rem_Fevereiro_CC", StringType(), True),
					 StructField("Vl_Rem_Marco_CC", StringType(), True),
					 StructField("Vl_Rem_Abril_CC", StringType(), True),
					 StructField("Vl_Rem_Maio_CC", StringType(), True),
					 StructField("Vl_Rem_Junho_CC", StringType(), True),
					 StructField("Vl_Rem_Julho_CC", StringType(), True),
					 StructField("Vl_Rem_Agosto_CC", StringType(), True),
					 StructField("Vl_Rem_Setembro_CC", StringType(), True),
					 StructField("Vl_Rem_Outubro_CC", StringType(), True),
					 StructField("Vl_Rem_Novembro_CC", StringType(), True),
					 StructField("Ano_Chegada_Brasil", StringType(), True)
])

# COMMAND ----------

arquivos = ['AC2016', 'AL2016', 'AM2016', 'AP2016', 'BA2016', 'CE2016', 'DF2016', 'ES2016', 'GO2016', 
            'MA2016', 'MG2016', 'MS2016', 'MT2016', 'PA2016', 'PB2016', 'PE2016', 'PI2016', 'PR2016', 'RJ2016', 
            'RN2016', 'RO2016', 'RR2016', 'RS2016', 'SC2016', 'SE2016', 'SP2016', 'TO2016']

# COMMAND ----------

# arquivos = ['AC2016']

# COMMAND ----------

# Read raw files 2016

for arquivo in arquivos:
    txt = f'{base}/{arquivo[-4:]}/{arquivo}.txt'
    #parquet = f'{arquivo}.parquet'
    df = spark.read.schema(schema_2016).load(txt, format="csv", header="true", delimiter=";", encoding="windows-1252")
    df = df.withColumn("Ano", lit(f"{arquivo[-4:]}"))
    df = df.withColumn("UF", lit(f"{arquivo[0:2]}"))
    
    df = df.withColumn("Ind_Trab_Parcial", lit("-1"))
    df = df.withColumn("Ind_Trab_Intermitente", lit("-1"))
    df = df.withColumn("Tipo_Salario", lit("-1"))
    df = df.withColumn("Vl_Salario_Contratual", lit("000000000,00"))
    
    df.write.parquet(f'{saida}/rais/Ano={arquivo[-4:]}/UF={arquivo[0:2]}')