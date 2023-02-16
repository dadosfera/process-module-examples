from snowflake.snowpark.functions import col, sum
from dadosfera import *
from snowflake.snowpark import functions as snowpark_functions
from snowflake.snowpark.types import IntegerType, StringType
from functools import reduce

session = Dadosfera().connect_to_snowpark()

table_partido = session.table("PUBLIC.TB__411SR9__PARTIDO_CANDIDATO_ESTADO"
                        ).select(col("NM_CANDIDATO").cast(StringType()).as_("NM_CANDIDATO"), 
                                 col("NM_PARTIDO").cast(StringType()).as_("NM_PARTIDO"), 
                                 col("NR_CANDIDATO").cast(IntegerType()).as_("NR_CANDIDATO"), 
                                 col("NR_PARTIDO").cast(IntegerType()).as_("NR_PARTIDO"), 
                                 col("SG_UF").cast(StringType()).as_("SIGLA_UF"))

table_partido.write.mode("overwrite").save_as_table("STAGING.MV_PARTIDO_CANDIDATO_ESTADO")
