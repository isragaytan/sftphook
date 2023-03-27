import pandas as pd
import zipfile
from zipfile import ZipFile, Path
from io import StringIO
from datetime import datetime


import pandas as pd

#27/07/2022|
#27/10/2022|
#168|
#01/07/2022|
# 31/07/2022|
# 525623264000|
# 1519901012|
# NETDR-MP-GR-NR BE 250M 30D FIFF|
# 701000000299811200|
# Byte|
# 29/09/2020|
# 07/07/2022|
# 06/08/2022|
# 10|
# 0|
# 0|
# 20|
# 0|
# 0|
# 0|
# 0|
# 0|
# 0|
# 0|
# 0|
# 0

arr_cols=['fecha_trans','fecha_medicion','cliente',
          'fecha_inicio_pf', 'fecha_fin_pf','msisdn',
          'offer_id', 'offer_name', 'free_unit_id', 
        'unidad_medida', 'fecha_inicial_act_uf','fecha_inicio_prod_primario',
        'fecha_fin_prod_prim', 'estado_tarificado', 'consumo_med_rgu_diario', 
        'dias_rgu_cambio_domic','dias_edo_act_rgu_ci', 'dias_edo_baja_rgu_ci', 
        'dias_edo_susp_rgu_ci','cons_med_acum_rguuf_ci_edo_act_10',
        'cons_med_acum_rguuf_ci_edo_baja_20', 
        'cons_med_acum_rguuf_ci_edo_susp_30', 'cons_med_acum_rgu_cd','cons_ex_cons_datos', 
        'unit_used_rr', 'unit_used_cumul_rr']

print(arr_cols)

def get_cols(csv):
  print("Reading csv")
  df = pd.read_csv(csv,sep='|')
  print(df.shape)
  df.columns=arr_cols
  print("Readed csv file...proceding to convert")
  df['fecha_trans']=pd.to_datetime(df['fecha_trans'].astype(str), format='%d/%m/%Y')
  df['fecha_medicion']=pd.to_datetime(df['fecha_medicion'].astype(str), format='%d/%m/%Y')
  df['fecha_inicio_pf']=pd.to_datetime(df['fecha_inicio_pf'].astype(str), format='%d/%m/%Y')
  df['fecha_fin_pf']=pd.to_datetime(df['fecha_fin_pf'].astype(str), format='%d/%m/%Y')
  df['fecha_inicial_act_uf']=pd.to_datetime(df['fecha_inicial_act_uf'].astype(str), format='%d/%m/%Y')
  df['fecha_inicio_prod_primario']=pd.to_datetime(df['fecha_inicio_prod_primario'].astype(str), format='%d/%m/%Y')
  df['fecha_fin_prod_prim']=pd.to_datetime(df['fecha_fin_prod_prim'].astype(str), format='%d/%m/%Y')
  #df = df.apply(lambda x:change_format(x),axis=0)
  #sdf.columns[0]=df.columns[0].apply(change_format)
  
  #df[1]= pd.to_datetime(df[1])
  df.to_csv(csv,index=False,sep='|',header=False)
  print("Writed csv succesfully")
  
#Change format
def change_format(strfrm):
    date_time_str = strfrm

    #Convert to datetime
    date_time_obj = datetime.strptime(date_time_str, '%d/%m/%Y')
    
    date_time_str =  date_time_obj.strftime('%Y-%m-%d') 
    
    print("La fecha formateada es",date_time_str)
  
#Process different zips
def process_file(file):
   
  with zipfile.ZipFile(file) as z:
   # open the csv file in the dataset
   with z.open("EstadoConsumo_168_20221023.csv") as f:
       
      # read the dataset
      train = pd.read_csv(f)
       
      # display dataset
      print(train.head())

if __name__=="__main__":
    get_cols("csv/EstadoConsumo_168_20221027.csv")
    #process_file("data/EstadoConsumo_168_20221023.7z")