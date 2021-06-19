# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit

import pandas as pd
import databricks.koalas as ks

# COMMAND ----------

# Lista de diretórios
base = '/dbfs/hackathon/extract/'
saida = '/mnt/trusted/'
schema = '/mnt/raw/schema/'

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

# arquivos =  ['AC2010', 'RR2010', 'AC2011', 'AC2012', 'AC2013', 'AC2014']

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

# Schema 2015

schema_2015 =   StructType([
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
					 StructField("Vl_Rem_Novembro_CC", StringType(), True)
])

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

# Schema 2017

schema_2017 =   StructType([
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
					 StructField("Ano_Chegada_Brasil", StringType(), True),
					 StructField("Ind_Trab_Parcial", StringType(), True),
					 StructField("Ind_Trab_Intermitente", StringType(), True)
])

# COMMAND ----------

# Schema 2018-2019

schema_2018_2019 =   StructType([
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
					 StructField("Ano_Chegada_Brasil", StringType(), True),
					 StructField("Ind_Trab_Parcial", StringType(), True),
					 StructField("Ind_Trab_Intermitente", StringType(), True),
					 StructField("Tipo_Salario", StringType(), True),
					 StructField("Vl_Salario_Contratual", StringType(), True)
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

# COMMAND ----------

arquivos = ['AC2015', 'AL2015', 'AM2015', 'AP2015', 'BA2015', 'CE2015', 'DF2015', 'ES2015', 'GO2015', 
            'MA2015', 'MG2015', 'MS2015', 'MT2015', 'PA2015', 'PB2015', 'PE2015', 'PI2015', 'PR2015', 'RJ2015', 
            'RN2015', 'RO2015', 'RR2015', 'RS2015', 'SC2015', 'SE2015', 'SP2015', 'TO2015']

# COMMAND ----------

# arquivos = ['AC2015']

# COMMAND ----------

# Read raw files 2015

for arquivo in arquivos:
    txt = f'{base}/{arquivo[-4:]}/{arquivo}.txt'
    #parquet = f'{arquivo}.parquet'
    df = spark.read.schema(schema_2015).load(txt, format="csv", header="true", delimiter=";", encoding="windows-1252")
    df = df.withColumn("Ano", lit(f"{arquivo[-4:]}"))
    df = df.withColumn("UF", lit(f"{arquivo[0:2]}"))
    
    df = df.withColumn("Ano_Chegada_Brasil", lit("-1"))
    df = df.withColumn("Ind_Trab_Parcial", lit("-1"))
    df = df.withColumn("Ind_Trab_Intermitente", lit("-1"))
    df = df.withColumn("Tipo_Salario", lit("-1"))
    df = df.withColumn("Vl_Salario_Contratual", lit("000000000,00"))
    
    df.write.parquet(f'{saida}/rais/Ano={arquivo[-4:]}/UF={arquivo[0:2]}')

# COMMAND ----------

arquivos = ['AC2016', 'AL2016', 'AM2016', 'AP2016', 'BA2016', 'CE2016', 'DF2016', 'ES2016', 'GO2016', 
            'MA2016', 'MG2016', 'MS2016', 'MT2016', 'PA2016', 'PB2016', 'PE2016', 'PI2016', 'PR2016', 'RJ2016', 
            'RN2016', 'RO2016', 'RR2016', 'RS2016', 'SC2016', 'SE2016', 'S2016P', 'TO2016']

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

# COMMAND ----------

arquivos = ['AC2017', 'AL2017', 'AM2017', 'AP2017', 'BA2017', 'CE2017', 'DF2017', 'ES2017', 'GO2017', 
            'MA2017', 'MG2017', 'MS2017', 'MT2017', 'PA2017', 'PB2017', 'PE2017', 'PI2017', 'PR2017', 'RJ2017', 
            'RN2017', 'RO2017', 'RR2017', 'RS2017', 'SC2017', 'SE2017', 'SP2017', 'TO2017']

# COMMAND ----------

# arquivos = ['AC2017']

# COMMAND ----------

# Read raw files 2017

for arquivo in arquivos:
    txt = f'{base}/{arquivo[-4:]}/{arquivo}.txt'
    #parquet = f'{arquivo}.parquet'
    df = spark.read.schema(schema_2017).load(txt, format="csv", header="true", delimiter=";", encoding="windows-1252")
    df = df.withColumn("Ano", lit(f"{arquivo[-4:]}"))
    df = df.withColumn("UF", lit(f"{arquivo[0:2]}"))
    
    df = df.withColumn("Tipo_Salario", lit("-1"))
    df = df.withColumn("Vl_Salario_Contratual", lit("0,00"))
    
    df.write.parquet(f'{saida}/rais/Ano={arquivo[-4:]}/UF={arquivo[0:2]}')

# COMMAND ----------

# Read UF dataframe

import pandas as pd

xls= pd.ExcelFile(f'{schema}RAIS_vinculos_layout2016.xls')
df_uf = pd.read_excel(xls, sheet_name='municipio')
df_uf = df_uf[0:5658]
df_uf['UF'] = df_uf['Município'].apply(lambda x: x.split("-")[0].split(":")[-1].upper())
df_uf['Num_Municipio'] = df_uf['Município'].apply(lambda x: x.split("-")[0].split(":")[0])
df_uf = df_uf.drop(['Município'], axis=1)
df_uf = spark.createDataFrame(df_uf)

# COMMAND ----------

# Create view UF

df_uf.createOrReplaceTempView("view_df_uf")

# COMMAND ----------

# Read raw files 2018

arquivos =  ['RAIS_VINC_PUB_NORTE_2018', 'RAIS_VINC_PUB_SP_2018', 'RAIS_VINC_PUB_SUL_2018', 'RAIS_VINC_PUB_NORDESTE_2018', 'RAIS_VINC_PUB_CENTRO_OESTE_2018', 'RAIS_VINC_PUB_MG_ES_RJ_2018']

# Read NORTE

estados_norte = ['RO', 'AM', 'RR', 'TO', 'PA', 'AC', 'AP']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_NORTE_2018.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_norte:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read SP
    
estados_sp = ['SP']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_SP_2018.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_sp:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read SUL
    
estados_sul = ['RS', 'SC', 'PR']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_SUL_2018.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_sul:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read NORDESTE
    
estados_nordeste = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_NORDESTE_2018.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_nordeste:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read CENTRO OESTE

estados_centro_oeste = ['DF', 'GO', 'MS', 'MT']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_CENTRO_OESTE_2018.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_centro_oeste:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read MG ES RJ
    
estados_mg_es_rj = ['MG', 'ES', 'RJ']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_MG_ES_RJ_2018.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_mg_es_rj:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# COMMAND ----------

# Read raw files 2019

arquivos =  ['RAIS_VINC_PUB_NORTE_2019', 'RAIS_VINC_PUB_SP_2019', 'RAIS_VINC_PUB_SUL_2019', 'RAIS_VINC_PUB_NORDESTE_2019', 'RAIS_VINC_PUB_CENTRO_OESTE_2019', 'RAIS_VINC_PUB_MG_ES_RJ_2019']

# Read NORTE

estados_norte = ['RO', 'AM', 'RR', 'TO', 'PA', 'AC', 'AP']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2019/RAIS_VINC_PUB_NORTE_2019.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2019"))
df.createOrReplaceTempView("view_df")

for estado in estados_norte:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2019/UF={estado}')

# Read SP

estados_sp = ['SP']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2019/RAIS_VINC_PUB_SP_2019.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2019"))
df.createOrReplaceTempView("view_df")

for estado in estados_sp:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2019/UF={estado}')

# Read SUL
    
estados_sul = ['RS', 'SC', 'PR']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2019/RAIS_VINC_PUB_SUL_2019.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2019"))
df.createOrReplaceTempView("view_df")

for estado in estados_sul:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2019/UF={estado}')

# Read NORDESTE    

estados_nordeste = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2019/RAIS_VINC_PUB_NORDESTE_2019.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2019"))
df.createOrReplaceTempView("view_df")

for estado in estados_nordeste:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2019/UF={estado}')

# Read CENTRO OESTE    
    
estados_centro_oeste = ['DF', 'GO', 'MS', 'MT']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2019/RAIS_VINC_PUB_CENTRO_OESTE_2019.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2019"))
df.createOrReplaceTempView("view_df")

for estado in estados_centro_oeste:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2019/UF={estado}')

# Read MG ES RJ

estados_mg_es_rj = ['MG', 'ES', 'RJ']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2019/RAIS_VINC_PUB_MG_ES_RJ_2019.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2019"))
df.createOrReplaceTempView("view_df")

for estado in estados_mg_es_rj:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2019/UF={estado}')

# COMMAND ----------

# Read parquet

parquetFile = spark.read.parquet("{saida}/rais/")

# COMMAND ----------

# Create view

parquetFile.createOrReplaceTempView("parquetFile")

# COMMAND ----------

# Clean data

clean = spark.sql("""
    SELECT
    Ano,
      REPLACE(LTRIM(REPLACE(Sexo_Trabalhador, '0', ' ')), ' ', '0') AS Sexo_Trabalhador,
      Idade,
      REPLACE(LTRIM(REPLACE(Escolaridade_apos_2005, '0', ' ')), ' ', '0') AS Escolaridade_apos_2005,
      CBO_Ocupacao_2002,
      CNAE_2_0_Classe,
      CNAE_2_0_Subclasse,
      Tipo_Vinculo,
      Qtd_Hora_Contr,
      CASE
        WHEN LENGTH(LTRIM(REPLACE(Vl_Remun_Media_Nom, '0', ' '))) > 3 THEN REPLACE(LTRIM(REPLACE(Vl_Remun_Media_Nom, '0', ' ')), ' ', '0')
        ELSE "0,00"
      END AS Vl_Remun_Media_Nom,
      Municipio
    FROM parquetFile
""")

# COMMAND ----------

# Create view

clean.createOrReplaceTempView("clean")

# COMMAND ----------

# Transform data

tratato = spark.sql("""
SELECT 
CAST(Ano AS INT),
Sexo_Trabalhador, 
       CAST(Idade AS INT), 
       CAST(Escolaridade_apos_2005 AS INT), 
       CNAE_2_0_Classe,
       CNAE_2_0_Subclasse,
       Tipo_Vinculo,
       CAST(Qtd_Hora_Contr AS INT),
       CAST(REPLACE(Vl_Remun_Media_Nom, ',', '.') AS FLOAT) AS Vl_Remun_Media_Nom,
       Municipio
       FROM clean
""")

# COMMAND ----------

# Create view tratado

tratato.createOrReplaceTempView("tratato")

# COMMAND ----------

# Pergunta 1

spark.sql("""
SELECT
Ano,
Sexo_Trabalhador,
AVG(Vl_Remun_Media_Nom)
FROM tratato
WHERE CNAE_2_0_Classe LIKE '62%'
GROUP BY Ano, Sexo_Trabalhador
ORDER BY Ano, Sexo_Trabalhador
""").show()

# COMMAND ----------

# Pergunta 2

spark.sql("""
SELECT
Ano,
Escolaridade_apos_2005,
AVG(Vl_Remun_Media_Nom)
FROM tratato
WHERE CNAE_2_0_Classe LIKE '016%'
GROUP BY Ano, Escolaridade_apos_2005
ORDER BY Ano, Escolaridade_apos_2005
""").show()