from snowflake.snowpark.functions import col, sum
from dadosfera import *
from snowflake.snowpark import functions as snowpark_functions
from snowflake.snowpark.types import IntegerType, StringType
from functools import reduce

session = Dadosfera().connect_to_snowpark(schema='STAGING')

view_partido_candidato = session.table("STAGING.MV_PARTIDO_CANDIDATO_ESTADO")

views_eleicoes_name = (
    session
    .sql('show tables')
    .select('"name"')
    .filter(snowpark_functions.col('"name"').like('MV%__ELEICOES_2022__%')).collect()
)

views_eleicoes = [session.table(f'STAGING.{view.name}'
                        ).select(col("NR_VOTAVEL"), 
                                 col("NM_VOTAVEL"), 
                                 col("DS_CARGO"), 
                                 col("NM_MUNICIPIO"), 
                                 col("NR_SECAO"), 
                                 col("NR_ZONA"), 
                                 col("SG_UF"), 
                                 col("NM_UE"), 
                                 col("QT_VOTOS")
                        ) for view in views_eleicoes_name]

views_eleicoes_final = [view.join(view_partido_candidato, (
                                    view_partido_candidato.col("NR_CANDIDATO") == view.col("NR_VOTAVEL")) & (
                                    view_partido_candidato.col("NM_CANDIDATO") == view.col("NM_VOTAVEL")) & (
                                    view_partido_candidato.col("SIGLA_UF") == view.col("SG_UF")), join_type="left"
                        ).group_by(col("NR_VOTAVEL"), 
                                   col("NR_PARTIDO"), 
                                   col("NM_PARTIDO"), 
                                   col("NM_VOTAVEL"), 
                                   col("DS_CARGO"), 
                                   col("NM_MUNICIPIO"), 
                                   col("NR_SECAO"), 
                                   col("NR_ZONA"), 
                                   col("SG_UF"), 
                                   col("NM_UE")
                        ).agg(sum("QT_VOTOS").alias("QT_VOTOS"))
                        for view, view_info in zip(views_eleicoes, views_eleicoes_name)]

view_eleicoes_final = reduce(lambda x, y: x.unionAll(y), views_eleicoes_final)
view_eleicoes_final.write.mode("overwrite").save_as_table('STAGING.MV_ELEICOES_BRASIL')
