import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get the job name from command line arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark context
sc = SparkContext()

# Initialize Glue context
glueContext = GlueContext(sc)

# Initialize Spark session
spark = glueContext.spark_session

# Initialize Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data from an S3 bucket using dynamic frame
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="caged_database",
    table_name="caged_csvuncompressed",
    transformation_ctx="S3bucket_node1",
)


# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("admitidos/desligados", "long", "admitidos_desligados", "long"),
        ("compet�ncia declarada", "long", "competencia_declarada", "long"),
        ("munic�pio", "long", "municipio", "long"),
        ("ano declarado", "long", "ano_declarado", "long"),
        ("cbo 2002 ocupa��o", "long", "cbo_2002_ocupacao", "long"),
        ("`cnae 1.0 classe`", "long", "cnae_1_0_classe", "long"),
        ("`cnae 2.0 classe`", "long", "cnae_2_0_classe", "long"),
        ("`cnae 2.0 subclas`", "long", "cnae_2_0_subclas", "long"),
        ("faixa empr in�cio jan", "long", "faixa_empr_inicio_jan", "long"),
        ("grau instru��o", "long", "grau_instrucao", "long"),
        ("qtd hora contrat", "long", "qtd_hora_contrat", "long"),
        ("ibge subsetor", "long", "ibge_subsetor", "long"),
        ("idade", "long", "idade", "long"),
        ("ind aprendiz", "long", "ind_aprendiz", "long"),
        ("ind portador defic", "long", "ind_portador_defic", "long"),
        ("ra�a cor", "long", "racacor", "long"),
        ("sal�rio mensal", "string", "salario_mensal", "string"),
        ("saldo mov", "long", "saldo_mov", "long"),
        ("sexo", "long", "sexo", "long"),
        ("tempo emprego", "string", "tempo_emprego", "string"),
        ("tipo estab", "long", "tipo_estab", "long"),
        ("tipo defic", "long", "tipo_defic", "long"),
        ("tipo mov desagregado", "long", "tipo_mov_desagregado", "long"),
        ("uf", "long", "uf", "long"),
        ("ind trab parcial", "long", "ind_trab_parcial", "long"),
        ("ind trab intermitente", "long", "ind_trab_intermitente", "long"),
        ("partition_0", "string", "partition_0", "string"),
        ("partition_1", "string", "partition_1", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Define sink for writing data to S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://caged-bucket/bronze/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=["partition_0"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)

# Set catalog information for the sink
S3bucket_node3.setCatalogInfo(
    catalogDatabase="caged_database", catalogTableName="bronze"
)

# Set output format to Glue Parquet
S3bucket_node3.setFormat("glueparquet")

# Write the transformed frame to the S3 bucket
S3bucket_node3.writeFrame(ApplyMapping_node2)

# Commit the Glue job
job.commit()
