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

# Create files list

anos = ['2010', '2011', '2012', '2013', '2014']

estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 
            'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']

arquivos = []

for estado in estados:
    for ano in anos:
        arquivos.append(estado+ano)

# COMMAND ----------

# Schema 2010-2014

schema_2010_2014 =   StructType([
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
					 StructField("Tipo_Vinculo", StringType(), True)
])

# COMMAND ----------

# Read raw files 2010-2014

for arquivo in arquivos:
    txt = f'{base}/{arquivo[-4:]}/{arquivo}.txt'
    # parquet = f'{arquivo}.parquet'
    df = spark.read.schema(schema_2010_2014).load(txt, format="csv", header="true", delimiter=";", encoding="windows-1252")
    df = df.withColumn("Ano", lit(f"{arquivo[-4:]}"))
    df = df.withColumn("UF", lit(f"{arquivo[0:2]}"))
    
    df = df.withColumn("Tipo_Vinculo", lit("-1"))
    df = df.withColumn("IBGE_Subsetor", lit("-1"))
    df = df.withColumn("Vl_Rem_Janeiro_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Fevereiro_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Marco_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Abril_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Maio_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Junho_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Julho_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Agosto_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Setembro_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Outubro_CC", lit("000000000,00"))
    df = df.withColumn("Vl_Rem_Novembro_CC", lit("000000000,00"))
    df = df.withColumn("Ano_Chegada_Brasil", lit("-1"))
    df = df.withColumn("Ind_Trab_Parcial", lit("-1"))
    df = df.withColumn("Ind_Trab_Intermitente", lit("-1"))
    df = df.withColumn("Tipo_Salario", lit("-1"))
    df = df.withColumn("Vl_Salario_Contratual", lit("000000000,00"))
    
    df.write.parquet(f'{saida}/rais/Ano={arquivo[-4:]}/UF={arquivo[0:2]}')