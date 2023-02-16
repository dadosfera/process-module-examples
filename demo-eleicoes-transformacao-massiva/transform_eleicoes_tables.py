from snowflake.snowpark.functions import col, sum
from dadosfera import *
from snowflake.snowpark import functions as snowpark_functions
from snowflake.snowpark.types import IntegerType, StringType
from functools import reduce

session = Dadosfera().connect_to_snowpark()

tables_eleicoes_name = (
    session
    .sql('show tables')
    .select('"name"')
    .filter(snowpark_functions.col('"name"').like('%__ELEICOES_2022__%')).collect()
)

views_eleicoes = [session.table(f'PUBLIC.{table.name}'
                        ).select(col("NR_VOTAVEL").cast(IntegerType()).as_("NR_VOTAVEL"), 
                                 col("NM_VOTAVEL").cast(StringType()).as_("NM_VOTAVEL"), 
                                 col("DS_CARGO").cast(StringType()).as_("DS_CARGO"), 
                                 col("NM_MUNICIPIO").cast(StringType()).as_("NM_MUNICIPIO"), 
                                 col("NR_SECAO").cast(IntegerType()).as_("NR_SECAO"), 
                                 col("NR_ZONA").cast(IntegerType()).as_("NR_ZONA"), 
                                 col("SG_UF").cast(StringType()).as_("SG_UF"), 
                                 col("NM_UE").cast(StringType()).as_("NM_UE"), 
                                 col("QT_VOTOS").cast(IntegerType()).as_("QT_VOTOS")
                        ).filter(~col("NM_VOTAVEL").startswith('Partido')
                        ).write.mode("overwrite").save_as_table(f'STAGING.{table.name.replace("TB","MV")}')
                        for table in tables_eleicoes_name]