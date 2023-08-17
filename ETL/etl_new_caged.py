
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when,create_map,lit,to_date, month, year
from pyspark.sql.types import StringType
from awsglue.job import Job
from itertools import chain
  
# Create or get Spark context
sc = SparkContext.getOrCreate()

# Initialize GlueContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)

# Read the parquet file from S3
df = spark.read.parquet('s3://caged-bucket/bronze/new_caged/')


# Perform data cleansing and transformation
df = df.withColumn("salario", regexp_replace(col("salario"), ",", "."))
df = df.withColumn("horascontratuais", regexp_replace(col("horascontratuais"), ",", "."))

df = df.withColumn("salario", col("salario").cast("double"))
df = df.withColumn("horascontratuais", col("horascontratuais").cast("int"))

df = df.withColumn("admissoes", when(col("saldomovimentacao") == "1", 1).otherwise(0))
df = df.withColumn("desligamentos", when(col("admissoes") == 0, 1).otherwise(0))

# Define columns to drop from the DataFrame
columns_to_drop = ["valorsalariofixo", "saldomovimentacao", "partition_0", 
                   "partition_1", "competenciadec", "tamestabjan"]

# Drop the specified columns
df = df.drop(*columns_to_drop)

# Read JSON mapping data from S3
data_frame = spark.read.json('s3://caged-bucket/mapping/new_caged_mapping/', multiLine=True)

# Create dimension DataFrame for location
dim_localizacao = df.select('municipio', 'uf', 'regiao').dropDuplicates(subset=['municipio'])

# Create mappings for different dimensions
sheet_names = ['municipio', 'uf', 'regiao']

# Iterate through the sheet names
for sheet in sheet_names:
    # Retrieve the mapping data for the current sheet
    code_to_label_map = data_frame.select(sheet).collect()[0][0].asDict()

    # Create a Spark SQL expression to map codes to their corresponding descriptions
    mapping_expr = create_map([lit(x) for x in chain(*code_to_label_map.items())])

    # Apply the mapping expression to the 'dim_localizacao' DataFrame
    dim_localizacao = dim_localizacao.withColumn(sheet + "_" + "descricao", mapping_expr[col(sheet)])

# Drop unnecessary columns and rename columns
dim_localizacao = dim_localizacao.drop('uf', 'regiao')
dim_localizacao = dim_localizacao.withColumnRenamed("municipio", "localizacao_id")
dim_localizacao = dim_localizacao.withColumnRenamed("municipio_descricao", "municipio")
dim_localizacao = dim_localizacao.withColumnRenamed("uf_descricao", "uf")
dim_localizacao = dim_localizacao.withColumnRenamed("regiao_descricao", "regiao")

# Cast the 'localizacao_id' column to the 'long' data type
dim_localizacao = dim_localizacao.withColumn("localizacao_id", col("localizacao_id").cast("long"))

# Create a DataFrame 'dim_ocupacao' with distinct 'cbo2002ocupacao', 'secao', and 'subclasse' columns
dim_ocupacao = df.select('cbo2002ocupacao', 'secao', 'subclasse').dropDuplicates(subset=['cbo2002ocupacao'])
sheet_names = ['cbo2002ocupacao', 'secao', 'subclasse']

# Iterate through the specified sheet names
for sheet in sheet_names:
    # Retrieve the mapping data for the current sheet
    code_to_label_map = data_frame.select(sheet).collect()[0][0].asDict()

    # Create a Spark SQL expression to map codes to their corresponding descriptions
    mapping_expr = create_map([lit(x) for x in chain(*code_to_label_map.items())])

    # Apply the mapping expression to the 'dim_ocupacao' DataFrame
    dim_ocupacao = dim_ocupacao.withColumn(sheet + "_" +"descricao", mapping_expr[col(sheet)])

# Drop unnecessary columns and rename columns
dim_ocupacao = dim_ocupacao.drop('secao', 'subclasse')
dim_ocupacao = dim_ocupacao.withColumnRenamed("cbo2002ocupacao", "ocupacao_id")
dim_ocupacao = dim_ocupacao.withColumnRenamed("cbo2002ocupacao_descricao", "ocupacao")
dim_ocupacao = dim_ocupacao.withColumnRenamed("secao_descricao", "secao")
dim_ocupacao = dim_ocupacao.withColumnRenamed("subclasse_descricao", "subclasse")
dim_ocupacao = dim_ocupacao.withColumn("ocupacao_id", col("ocupacao_id").cast("long"))

# Create a DataFrame 'fato_caged' by dropping specified columns
fato_caged = df.drop('uf','regiao','secao','subclasse')

# Convert 'competenciamov' column to date format
conversion_expr = to_date(fato_caged["competenciamov"], "yyyyMM")
fato_caged = fato_caged.withColumn("data_movimentacao", conversion_expr)

# Drop and rename columns, and cast specified columns to their intended data types
fato_caged = fato_caged.drop('competenciamov')
fato_caged = fato_caged.withColumn("mes", month(col("data_movimentacao")))
fato_caged = fato_caged.withColumn("ano", year(col("data_movimentacao")))
column_type_mapping = {
    "municipio": "long",
    "cbo2002ocupacao": "long",
    "ano": "string",
    "mes": "string"
}

# Apply data type conversions to specified columns
for col_name, data_type in column_type_mapping.items():
    fato_caged = fato_caged.withColumn(col_name, col(col_name).cast(data_type))

sheet_names = ["sexo","racacor","graudeinstrucao","origemdainformacao","tipoestabelecimento","tipoempregador","tipomovimentacao",
    "tipodedeficiencia","indtrabintermitente","indtrabparcial","indicadoraprendiz","indicadordeforadoprazo",
    "unidadesalariocodigo", "categoria"]

# Iterate through the specified sheet names again for data mapping
for sheet in sheet_names:
    # Retrieve the mapping data for the current sheet
    code_to_label_map = data_frame.select(sheet).collect()[0][0].asDict()

    # Create a Spark SQL expression to map codes to their corresponding descriptions
    mapping_expr = create_map([lit(x) for x in chain(*code_to_label_map.items())])

   # Apply the mapping expression to the 'fato_caged' DataFrame
    fato_caged = fato_caged.withColumn(sheet, mapping_expr[col(sheet)])

# Define a mapping between old and new column names
mapeamento_colunas = {
    "municipio": "localizacao_id",
    "cbo2002ocupacao": "ocupacao_id",
    "origemdainformacao": "origem_da_informacao",
    "tipoestabelecimento": "tipo_estabelecimento",
    "tipoempregador": "tipo_empregador",
    "tipomovimentacao": "tipo_movimentacao",
    "tipodedeficiencia": "tipo_de_deficiencia",
    "indtrabintermitente": "ind_trab_intermitente",
    "indtrabparcial": "ind_trab_parcial",
    "indicadoraprendiz": "ind_aprendiz",
    "indicadordeforadoprazo": "ind_de_fora_doprazo",
    "unidadesalariocodigo": "unidade_salario"
}

# Rename columns in 'fato_caged' based on the mapping
for nome_antigo, nome_novo in mapeamento_colunas.items():
    fato_caged = fato_caged.withColumnRenamed(nome_antigo, nome_novo)

# Write transformed DataFrames to Parquet format with specified partitions
fato_caged.write.partitionBy("ano", "mes").parquet("s3://caged-bucket/silver/new_caged_transformed/tables/tabela_fato_caged", mode="overwrite")
dim_localizacao.write.partitionBy("regiao","uf").parquet("s3://caged-bucket/silver/new_caged_transformed/tables/tabela_dim_localizacao", mode="overwrite")
dim_ocupacao.write.partitionBy("secao").parquet("s3://caged-bucket/silver/new_caged_transformed/tables/tabela_dim_ocupacao", mode="overwrite")
job.commit()