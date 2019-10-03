import re
from pyspark.sql import Window
from pyspark.sql.functions import regexp_extract, col, countDistinct, rank,  unix_timestamp, from_unixtime

#faz a leitura dos arquivos
df = spark.read.text("/FileStore/tables/access_log_Aug95")
df2 = spark.read.text("/FileStore/tables/access_log_Jul95")

df = df.union(df2)
df.cache()

#definições das expressões regulares
host_regex = r'(^\S+\.[\S+\.]+\S+)\s'
timestamp_regex = r'\[(.*?)\]' 
requisicao_regex = r'\s/(.*?)\s' 
retorno_http_regex = r'\s(\d{3})\s'
bytes_ret_regex =  r'\s(\d+)$'

#cria um novo dataframe  separando os dados em colunas usando expressão regular
colunas_df =   df.select(regexp_extract('value', host_regex, 1).alias('host'),
                         regexp_extract('value', timestamp_regex, 1).substr(1,11).alias('timestamp'),
                         from_unixtime(unix_timestamp(regexp_extract('value', timestamp_regex, 1).substr(1,11),"dd/MMM/yyyy")).alias('data'),
                         regexp_extract('value', requisicao_regex, 1).alias('requisicao'),
                         regexp_extract('value', retorno_http_regex, 1).cast('integer').alias('status'),
                         regexp_extract('value', bytes_ret_regex, 1).cast('integer').alias('bytes_ret'))



print ("###################INI DAS RESPOSTAS###################")
#Resposta 1
print ("1. Número de hosts únicos.")

print (colunas_df.select(countDistinct("host")).show())
#fim resposta 1

#resposta 2
print ("2. O total de erros 404.")

print (colunas_df.filter("status = 404").count())

#fim respota 2

#respota 3
print ("3. Os 5 URLs que mais causaram erro 404")

colunas_df.filter("status = 404").groupby('requisicao').count().orderBy('count', ascending=False).show(5)



#fim respota 3

#resposta 4

print ("4. Quantidade de erros 404 por dia.")

print (colunas_df.filter("status = 404").groupBy("data").count().orderBy("data").show(62))

#fim resposta 4

#resposta 5
print ("5. O total de bytes retornados.")
print ("maiores sites com  erros 404")

print (colunas_df.groupBy().sum("bytes_ret").collect())

print ("###################FIM DAS RESPOSTAS###################")
