# Databricks notebook source
# Instalando biblioteca secundaria
!pip install xlrd

# COMMAND ----------

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

# Read UF dataframe
# dbfs:/mnt/raw/schema/RAIS_vinculos_layout2016.xls
import pandas as pd

xls= pd.ExcelFile(f'{schema}/RAIS_vinculos_layout2016.xls')
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

arquivos =  ['RAIS_VINC_PUB_NORTE', 'RAIS_VINC_PUB_SP', 'RAIS_VINC_PUB_SUL', 'RAIS_VINC_PUB_NORDESTE', 'RAIS_VINC_PUB_CENTRO_OESTE', 'RAIS_VINC_PUB_MG_ES_RJ']

# Read NORTE

estados_norte = ['RO', 'AM', 'RR', 'TO', 'PA', 'AC', 'AP']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_NORTE.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_norte:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read SP
    
estados_sp = ['SP']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_SP.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_sp:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read SUL
    
estados_sul = ['RS', 'SC', 'PR']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_SUL.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_sul:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read NORDESTE
    
estados_nordeste = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_NORDESTE.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_nordeste:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read CENTRO OESTE

estados_centro_oeste = ['DF', 'GO', 'MS', 'MT']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_CENTRO_OESTE.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_centro_oeste:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')

# Read MG ES RJ
    
estados_mg_es_rj = ['MG', 'ES', 'RJ']

df = spark.read.schema(schema_2018_2019).load(f"{base}/2018/RAIS_VINC_PUB_MG_ES_RJ.txt", format="csv", header="true", delimiter=";", encoding="windows-1252")
df = df.withColumn("Ano", lit("2018"))
df.createOrReplaceTempView("view_df")

for estado in estados_mg_es_rj:
    df_teste = spark.sql(f"SELECT * FROM view_df AS df INNER JOIN view_df_uf AS uf_df ON df.Municipio = uf_df.Num_Municipio WHERE UF = '{estado}'")
    df_teste = df_teste.drop("Num_Municipio")
    df_teste.write.parquet(f'{saida}/rais/Ano=2018/UF={estado}')