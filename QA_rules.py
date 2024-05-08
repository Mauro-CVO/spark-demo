import os
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id
import pyspark.sql.functions as F


def name_change(path, new_name):
    files = os.listdir(path)

    for file in files:
        if file.startswith("part"):
            os.rename(path + file, new_name)


spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)  # Property used to format output tables better

# Load a CSV file into a Spark DataFrame
df = spark.read.csv('data.csv', header=True, inferSchema=True)

# Print the schema of the DataFrame
df.printSchema()

# Print the first few rows of the DataFrame
df.show(5)

#Agregar indices

df = df.withColumn("ID", row_number().over(Window.orderBy(monotonically_increasing_id())))

df.show()

# Numero de Registros con valores nulos por columna

# Get the number of null values in each column
null_counts = df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df.columns])

# Print the results
null_counts.show()

cols = [F.col(c) for c in df.columns]

## DF con valores no nulos
df_not_nulls = df.select(cols).filter(F.col("Nombre").isNotNull() &
                                      F.col("Apellidos").isNotNull() &
                                      F.col("Fecha_Nacimiento").isNotNull() &
                                      F.col("Lugar_Nacimiento").isNotNull() &
                                      F.col("CURP").isNotNull() &
                                      F.col("RFC").isNotNull() &
                                      F.col("CP").isNotNull() &
                                      F.col("RFC").isNotNull() &
                                      F.col("Calle").isNotNull() &
                                      F.col("Colonia").isNotNull() &
                                      F.col("Estado").isNotNull() &
                                      F.col("Ciudad").isNotNull() &
                                      F.col("Telefono").isNotNull() &
                                      F.col("Correo Electronico").isNotNull() &
                                      F.col("Sexo").isNotNull() &
                                      F.col("Entidad_Procedencia").isNotNull())
df_not_nulls.show(4)
df_not_nulls.count()

## Campos que tiene RFC y CURP con longitudes menores o mayores a 13 y 18 respectivamente

df_not_rfc_curp = df_not_nulls.select(cols).filter((F.length(F.col("CURP")) != 8) |
                                                   (F.length(F.col("RFC")) != 5))

df_not_rfc_curp.show(4)
df_not_rfc_curp.count()

# Personas Menores a 65 años

# Convert the birthday column to a date type
df_date = df_not_rfc_curp.withColumn("Fecha_Nacimiento", F.to_date("Fecha_Nacimiento", "d/M/y"))

# Calculate the age of each person in days
df_date_dias = df_date.withColumn("Dias_Edad", F.datediff(F.current_date(), df_date["Fecha_Nacimiento"]))

# filter rows with age less than 65 years old
df_less_than_65 = df_date_dias.filter(df_date_dias["Dias_Edad"] < 23730)

df_less_than_65.show()
df_less_than_65.count()

# prompt: Left anti join with two dataframes in pyspark

# Left anti join
df_not_validated = df.join(df_less_than_65, on=['ID'], how='left_anti')

# Print the results
df_not_validated.show(4)
df_not_validated.count()

df_validated = df_less_than_65.select(cols)

df_validated.count()

# prompt: save df as CSV pyspark
df_validated.write.option("header", True) \
    .csv("validados")

df_not_validated.write.option("header", True) \
    .csv("no_validados")

name_V = "validados.csv"
name_NV = "no_validados.csv"
path_V = "validados/"
path_NV = "no_validados/"

name_change(path_V, name_V)
name_change(path_NV, name_NV)
