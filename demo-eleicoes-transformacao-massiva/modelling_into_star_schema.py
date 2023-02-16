from dadosfera import Dadosfera
from snowflake.snowpark.functions import col, sum
from snowflake.snowpark.window import Window
from snowflake.snowpark import functions as snowpark_functions

session = Dadosfera().connect_to_snowpark()

mv_eleicoes = session.table('STAGING.MV_ELEICOES_BRASIL')
w = Window.orderBy("NR_VOTAVEL","NM_VOTAVEL","NR_PARTIDO","NM_PARTIDO","DS_CARGO","NM_MUNICIPIO","NR_SECAO","NR_ZONA","SG_UF","NM_UE","QT_VOTOS")
mv_eleicoes = mv_eleicoes.withColumn('ID', snowpark_functions.row_number().over(w))

view_localizacao = mv_eleicoes.select(col("NM_MUNICIPIO"), col("NR_SECAO"), col("NR_ZONA"), col("SG_UF"), col("NM_UE"))
view_localizacao = view_localizacao.drop_duplicates()
w = Window.orderBy("NM_MUNICIPIO","NR_SECAO","NR_ZONA","SG_UF","NM_UE")
view_localizacao = view_localizacao.withColumn('ID_LOCALIZACAO', snowpark_functions.row_number().over(w))

view_votavel = mv_eleicoes.select(col("NR_VOTAVEL"), col("NR_PARTIDO"), col("NM_PARTIDO"), col("NM_VOTAVEL"), col("DS_CARGO"))
view_votavel = view_votavel.drop_duplicates()
w = Window.orderBy("NR_VOTAVEL","NR_PARTIDO","NM_PARTIDO","NM_VOTAVEL","DS_CARGO")
view_votavel = view_votavel.withColumn('ID_VOTAVEL', snowpark_functions.row_number().over(w))

view_eleicoes_final = mv_eleicoes.join(view_localizacao, (mv_eleicoes.col("NM_MUNICIPIO") == view_localizacao.col("NM_MUNICIPIO")) & (
                                                            mv_eleicoes.col("SG_UF")      == view_localizacao.col("SG_UF")) & (
                                                            mv_eleicoes.col("NM_UE")      == view_localizacao.col("NM_UE")) & (
                                                            mv_eleicoes.col("NR_SECAO")   == view_localizacao.col("NR_SECAO")) & (
                                                            mv_eleicoes.col("NR_ZONA")    == view_localizacao.col("NR_ZONA")), join_type="left"
                                    ).join(view_votavel, (mv_eleicoes.col("NR_VOTAVEL")   == view_votavel.col("NR_VOTAVEL")) & (
                                                            mv_eleicoes.col("NM_VOTAVEL") == view_votavel.col("NM_VOTAVEL")) & (
                                                            mv_eleicoes.col("NR_PARTIDO") == view_votavel.col("NR_PARTIDO")) & (
                                                            mv_eleicoes.col("NM_PARTIDO") == view_votavel.col("NM_PARTIDO")) & (
                                                            mv_eleicoes.col("DS_CARGO")   == view_votavel.col("DS_CARGO")), join_type="left"
                                    ).group_by(col("ID"), 
                                               col("ID_LOCALIZACAO"),
                                               col("ID_VOTAVEL"),
                                               col("QT_VOTOS")
                                    ).agg()

view_eleicoes_final.write.mode("overwrite").save_as_table('STAGING.MV_STARSCHEMA_ELEICOES_BRASIL')
view_localizacao.write.mode("overwrite").save_as_table('STAGING.MV_STARSCHEMA_LOCALIZACAO')
view_votavel.write.mode("overwrite").save_as_table('STAGING.MV_STARSCHEMA_VOTAVEL')
