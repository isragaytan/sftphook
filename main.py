
import os
from sys import prefix
import subprocess
import os 
from typing import Counter
import time
from pyunpack import Archive

import pymysql
import pandas as pd
import numpy as np
import warnings
import py7zr
import pysftp
import patoolib
import datetime

import pymysql
import pandas as pd
from pandas.errors import EmptyDataError
import numpy as np
from notifier import messageDiscord

# ENVIRONMENT VARIABLES
DB_HOST_WIWI_MS= os.environ.get('DB_HOST_WIWI_MS')
DB_USER_WIWI_MS=os.environ.get('DB_USER_WIWI_MS')
DB_PASSWORD_WIWI_MS = os.environ.get('DB_PASSWORD_WIWI_MS')
DB_DATABASE_NAME_WIWI_MS = os.environ.get('DB_DATABASE_NAME_WIWI_MS')
ALTAN_SFTP_USER=os.environ.get("ALTAN_SFTP_USER")
ALTAN_SFTP_PASSWORD=os.environ.get("ALTAN_SFTP_PASSWORD")

# WARNINGS
warnings.filterwarnings('ignore','.*Failed to load HostKeys.*')

# RDS MYSQL CONNCECTION
cnx = pymysql.connect(host=DB_HOST_WIWI_MS,
                             user=DB_USER_WIWI_MS,
                             password=DB_PASSWORD_WIWI_MS ,
                             database="altan_seq",
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor,
                             connect_timeout=3153600,read_timeout=3153600,write_timeout=3153600
                             )

# Array for failure dates
arr_failure_dates=[]

# Array for 24 lenght
arr_cols_24=['fecha_trans','fecha_medicion','cliente',
          'fecha_inicio_pf', 'fecha_fin_pf','msisdn',
          'offer_id', 'offer_name', 'free_unit_id', 
        'unidad_medida', 'fecha_inicial_act_uf','fecha_inicio_prod_primario',
        'fecha_fin_prod_prim', 'estado_tarificado', 'consumo_med_rgu_diario', 
        'dias_rgu_cambio_domic','dias_edo_act_rgu_ci', 'dias_edo_baja_rgu_ci', 
        'dias_edo_susp_rgu_ci','cons_med_acum_rguuf_ci_edo_act_10',
        'cons_med_acum_rguuf_ci_edo_baja_20', 
        'cons_med_acum_rguuf_ci_edo_susp_30', 'cons_med_acum_rgu_cd','cons_ex_cons_datos']

# Array for headers
arr_cols_25=['fecha_trans','fecha_medicion','cliente',
          'fecha_inicio_pf', 'fecha_fin_pf','msisdn',
          'offer_id', 'offer_name', 'free_unit_id', 
        'unidad_medida', 'fecha_inicial_act_uf','fecha_inicio_prod_primario',
        'fecha_fin_prod_prim', 'estado_tarificado', 'consumo_med_rgu_diario', 
        'dias_rgu_cambio_domic','dias_edo_act_rgu_ci', 'dias_edo_baja_rgu_ci', 
        'dias_edo_susp_rgu_ci','cons_med_acum_rguuf_ci_edo_act_10',
        'cons_med_acum_rguuf_ci_edo_baja_20', 
        'cons_med_acum_rguuf_ci_edo_susp_30', 'cons_med_acum_rgu_cd','cons_ex_cons_datos', 
        'unit_used_rr']

# Array for headers
arr_cols_26=['fecha_trans','fecha_medicion','cliente',
          'fecha_inicio_pf', 'fecha_fin_pf','msisdn',
          'offer_id', 'offer_name', 'free_unit_id', 
        'unidad_medida', 'fecha_inicial_act_uf','fecha_inicio_prod_primario',
        'fecha_fin_prod_prim', 'estado_tarificado', 'consumo_med_rgu_diario', 
        'dias_rgu_cambio_domic','dias_edo_act_rgu_ci', 'dias_edo_baja_rgu_ci', 
        'dias_edo_susp_rgu_ci','cons_med_acum_rguuf_ci_edo_act_10',
        'cons_med_acum_rguuf_ci_edo_baja_20', 
        'cons_med_acum_rguuf_ci_edo_susp_30', 'cons_med_acum_rgu_cd','cons_ex_cons_datos', 
        'unit_used_rr', 'unit_used_cumul_rr']


# Connect to SFTP and bring files
def do_sftp():
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection('35.171.213.230', username=ALTAN_SFTP_USER, password=ALTAN_SFTP_PASSWORD,cnopts=cnopts) as sftp:
            
            print("Connection success!")
            print("Current directory", sftp.pwd)
            #print(sftp.listdir())
            arr_files=[]
            
            #Download SFTP FILES 
            with sftp.cd("/168/facturacion/"):
                files = sftp.listdir()
                #Provisional solution for 2022 and 2023 fix it
                print("Los archivos son de files directo ",files)
                files_estados = [x[-29:] for x in files if "EstadoConsumo_168_2023" in x ]
                print("FILES ESTADOS",files_estados)
                for file in files_estados:
                    #print(file)
                    sftp.get(file, './data/'+file)
                    arr_files.append(file)
            print("Arreglo final en files estados",arr_files)

           #Get S3 7Z files
            s3_files = get_wiwi_s3_files()
            #print("S3 FILES ", s3_files)  
              
            #process_extensions(s3_files)
            #print("SFTP CLEAN FILES",arr_files)
            
            final_array = get_new_files(arr_files,s3_files)

            #If there are new files..    
            if final_array:
            
                print("THE FINAL ARRAY IS",final_array)
                 #upload raw 7z files 
                start = time.perf_counter()   

                #UPLOAD NEW FILES
                upload_files(final_array,"data")
                #start = time.perf_counter()
               
                #Test multiprocessing for just upload files
                
                #processes = [multiprocessing.Process(target=upload_files, args=[final_array,"data"])]
     
                #extract files
                extract_files(final_array)
                
                csv_array=[]
                for j in final_array:
                    csv_ext = j.replace(".7z",".csv")
                    csv_array.append(csv_ext)
                #read csv directory
                csvs = read_csv_directory("csv")
                #print("Length for the csv ",len(csvs))
                
                
                #Transform csvs
                transform_csv(csvs,True)
                
                #upload csv files
                upload_files(csvs,"csv")
                
                
                #process_files(final_array)
                process_files(csvs)
                
                print("Files with failure ", arr_failure_dates)
                delete_files(csvs)
                finish = time.perf_counter()
                print(f'Taked  {finish-start: .2f} second(s) to upload files ' ,str(len(final_array)))
            else:
                print("No new  files added") 
    
    except Exception as ex:
        print(ex)
        messageDiscord("ETL ALTAN SFTP" , "Error running ALTAN SFTP ETL"+ ex)

# FUNCTION TO RUN ONLY ONCE!!!
def process_batch_dir():
    print("processing batch")
    # folder path
    dir_path = "/home/ubuntu/altansftp/csvback/"

    # list to store files
    res = []
    procs = []
#    Iterate directory
    for path in os.listdir(dir_path):
    # check if current path is a file
        print("El path es ",path)
        if os.path.isfile(os.path.join(dir_path, path)):
            res.append(path)
            
    transform_csv(res,False)
    upload_files(res,dir_path)
    process_files(res)

# DELETE CSV FILES
def delete_files(files):
    print("Starting delete files ",files)

    os.remove(files)
    print("Deleted file ",files)
    
# TRANSFORM CSVS
def transform_csv(csv,batch=False):
    #print("Transformig csv: ",csv)
    if batch:
        dir_path="csv/"
    else:
        dir_path="/home/ubuntu/altansftp/csvback/"

    name_file = ""    
    for j in csv:
        print("Transforming csv:",j)
        name_file = j
        try:
            df =  pd.read_csv(dir_path+j,sep='|')
            if len(df.columns) != 0: 
                arr_assing=[]
               
                num_columns=len(df.columns)
               
                print("CSV :",j," Longitud de campos :",num_columns)
                if(num_columns==24):
                    arr_assing=arr_cols_24
                if(num_columns==25):
                    arr_assing=arr_cols_25
                if(num_columns==26):
                    arr_assing=arr_cols_26
                
                
                df.columns=arr_assing
                #print("Readed csv file...proceding to convert")
                #print(df['fecha_trans'],df['fecha_medicion'])
                try:
                    
                    df['fecha_trans']=pd.to_datetime(df['fecha_trans'].astype(str), format='%d/%m/%Y')
                    df['fecha_medicion']=pd.to_datetime(df['fecha_medicion'].astype(str), format='%d/%m/%Y')
                    df['fecha_inicio_pf']=pd.to_datetime(df['fecha_inicio_pf'].astype(str), format='%d/%m/%Y')
                    df['fecha_fin_pf']=pd.to_datetime(df['fecha_fin_pf'].astype(str), format='%d/%m/%Y')
                    df['fecha_inicial_act_uf']=pd.to_datetime(df['fecha_inicial_act_uf'].astype(str), format='%d/%m/%Y')
                    df['fecha_inicio_prod_primario']=pd.to_datetime(df['fecha_inicio_prod_primario'].astype(str), format='%d/%m/%Y')
                    df['fecha_fin_prod_prim']=pd.to_datetime(df['fecha_fin_prod_prim'].astype(str), format='%d/%m/%Y')
                   
                    #df['fecha_trans']=pd.to_datetime(df['fecha_trans'].astype(str), format='%Y-%m-%d')
                    #df['fecha_medicion']=pd.to_datetime(df['fecha_medicion'].astype(str), format='%Y-%m-%d')
                    #df['fecha_inicio_pf']=pd.to_datetime(df['fecha_inicio_pf'].astype(str), format='%Y-%m-%d')
                    #df['fecha_fin_pf']=pd.to_datetime(df['fecha_fin_pf'].astype(str), format='%Y-%m-%d')
                    #df['fecha_inicial_act_uf']=pd.to_datetime(df['fecha_inicial_act_uf'].astype(str), format='%Y-%m-%d')
                    #df['fecha_inicio_prod_primario']=pd.to_datetime(df['fecha_inicio_prod_primario'].astype(str), format='%Y-%m-%d')
                    #df['fecha_fin_prod_prim']=pd.to_datetime(df['fecha_fin_prod_prim'].astype(str), format='%Y-%m-%d')

        #df = df.apply(lambda x:change_format(x),axis=0)
        #sdf.columns[0]=df.columns[0].apply(change_format)
                except Exception as ex:
                    print("Exception ocurred on",ex, "File :", j)
                    arr_failure_dates.append(j)
        except EmptyDataError:
            print("No columns to parse from file",j)            
  #df[1]= pd.to_datetime(df[1])
        df.insert(num_columns,"gb",name_file)
        #df.insert(num_columns+1,"fromfile",csv)
        df.to_csv(dir_path+j,index=False,sep='|',header=True)
        print("Tranformed CSV Succesfully :",j)
    
    print("Failed files", arr_failure_dates)

# Read Directory    
def read_csv_directory(path):
     csvs = []
     for p in os.listdir(path):
         print("En directorio ",p)
         csvs.append(p)
    
     return csvs

# Process diferent zip files
def process_extensions(files):
    print("Separating files")
    arr_edos = [] 
    for j in files:
        str_coin = j
        str_es = str_coin.find("EstadoConsumo")
        if(str_es):
            arr_edos.append(j)
    
    print("El arreglo de los estados es")           
    return arr_edos

# Upload files
def upload_files(files,dirfrom):
    print("Starting uploading files :",files)

    if dirfrom == "csv": 
        for file in files:
            print("File:",file)
            #clean_file=clean_data(file)
            process = subprocess.run(['aws', 's3', 'cp','./'+dirfrom+'/'+file,'s3://altan-data/sftp/'+file])
    elif dirfrom == "/home/ubuntu/altansftp/csvback/":
        for file in files:
            print("File:",file)
            #clean_file=clean_data(file)
            process = subprocess.run(['aws', 's3', 'cp',dirfrom+file,'s3://altan-data/sftp/'+file])
    else:
        for file in files:
            print("File:",file)
            #clean_file=clean_data(file)
            process = subprocess.run(['aws', 's3', 'cp','./'+dirfrom+'/'+file,'s3://altan-data/sftp/'+file])

# Exctract 7z files
def extract_files(files):
    for f in files:
        print("Extracting 7z files ",f)
        Archive("data/"+f).extractall("csv/")
    print("Completed extracted files")

# Process files
def process_files(files):
    print("Reading CSV :",files)
    #csv_data = pd.read_csv(files,sep="|")
    #print("Readed succesfully csv")
    #engine = create_engine("mysql+pymysql://"+DB_USER_WIWI_MS+":"+DB_PASSWORD_WIWI_MS+"@"+DB_HOST_WIWI_MS+"/altan_seq")
    
    #print("Created engine succesfully")
    # "fecha_transaccion,msisdn,offerrid,offername,consumo_medido,dias_edo_activo_rgu_ci,fecha_inicio,gb,caso,msidn_2,ruta,rotulo,asignado,problema,bolsas_fututas,altan_ofertas,oferta,100_pct,80_pct,consumo,disponible,promedio,1_dia_consumo,2_dia_consumo,disponible_2_dias,consumo30_dias,bolsas50_gb,bolsas,promedio_util_30_dias,bolsas50_gb_b,bolsas_b,actividad_90_dias,nombre_empresa"
    # `idsftp` INT NOT NULL AUTO_INCREMENT,

    #sql = "INSERT INTO seq(fecha_hora,msidn,imei,imsi,app_user,consumo_app,marca_handset,modelo_handset,red_conn_off,ubicacion)"
    cursor = cnx.cursor()
    
    for file in files:
        
         sql_load_s3 ="LOAD DATA FROM S3 's3://altan-data/sftp/%s' INTO TABLE sftp_hook FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (fecha_trans,fecha_medicion,cliente, fecha_inicio_pf, fecha_fin_pf,msisdn, offer_id, offer_name, free_unit_id, \
        unidad_medida, fecha_inicial_act_uf, fecha_inicio_prod_prim, fecha_fin_prod_prim, estado_tarificado, consumo_med_rgu_diario, dias_rgu_cambio_domic, \
        dias_edo_act_rgu_ci, dias_edo_baja_rgu_ci, dias_edo_susp_rgu_ci,cons_med_acum_rguuf_ci_edo_act_10, cons_med_acum_rguuf_ci_edo_baja_20, \
        cons_med_acum_rguuf_ci_edo_susp_30, cons_med_acum_rgu_cd, cons_ex_cons_datos, unit_used_rr, unit_used_cumul_rr,fromfile)" %(file)
         
         print(sql_load_s3)
                
         try:
            #cursor.execute(sql_truncate)
            cursor.execute(sql_load_s3)
            cnx.commit()
         except Exception as ex:
            print(ex)
            messageDiscord("ETL ALTAN SFTP" , "Error running ALTAN SFTP ETL"+ ex)
    
    print("Successfully inserted data")
    messageDiscord("ETL ALTAN SFTP" , "Success running ALTAN SFTP ETL")
    

def process_file_forced(files):
    print("Reading CSV :",files)
    #csv_data = pd.read_csv(files,sep="|")
    #print("Readed succesfully csv")
    #engine = create_engine("mysql+pymysql://"+DB_USER_WIWI_MS+":"+DB_PASSWORD_WIWI_MS+"@"+DB_HOST_WIWI_MS+"/altan_seq")
    
    #print("Created engine succesfully")
    # "fecha_transaccion,msisdn,offerrid,offername,consumo_medido,dias_edo_activo_rgu_ci,fecha_inicio,gb,caso,msidn_2,ruta,rotulo,asignado,problema,bolsas_fututas,altan_ofertas,oferta,100_pct,80_pct,consumo,disponible,promedio,1_dia_consumo,2_dia_consumo,disponible_2_dias,consumo30_dias,bolsas50_gb,bolsas,promedio_util_30_dias,bolsas50_gb_b,bolsas_b,actividad_90_dias,nombre_empresa"
    # `idsftp` INT NOT NULL AUTO_INCREMENT,

    #sql = "INSERT INTO seq(fecha_hora,msidn,imei,imsi,app_user,consumo_app,marca_handset,modelo_handset,red_conn_off,ubicacion)"
    cursor = cnx.cursor()
    
    for file in files:
        
         sql_load_s3 ="LOAD DATA FROM S3 's3://altan-data/sftp/%s' INTO TABLE sftp_hook FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (fecha_trans,fecha_medicion,cliente, fecha_inicio_pf, fecha_fin_pf,msisdn, offer_id, offer_name, free_unit_id, \
        unidad_medida, fecha_inicial_act_uf, fecha_inicio_prod_prim, fecha_fin_prod_prim, estado_tarificado, consumo_med_rgu_diario, dias_rgu_cambio_domic, \
        dias_edo_act_rgu_ci, dias_edo_baja_rgu_ci, dias_edo_susp_rgu_ci,cons_med_acum_rguuf_ci_edo_act_10, cons_med_acum_rguuf_ci_edo_baja_20, \
        cons_med_acum_rguuf_ci_edo_susp_30, cons_med_acum_rgu_cd, cons_ex_cons_datos, unit_used_rr, unit_used_cumul_rr,fromfile)" %(file)
         
         print(sql_load_s3)
                
         try:
            #cursor.execute(sql_truncate)
            cursor.execute(sql_load_s3)
            cnx.commit()
         except Exception as ex:
            print(ex)
            messageDiscord("ETL ALTAN SFTP" , "Error running ALTAN SFTP ETL"+ ex)
    
    print("Successfully inserted data")
    messageDiscord("ETL ALTAN SFTP" , "Success running ALTAN SFTP ETL")

#Get string differences
#Clean data
def clean_data(strdata):

    res =  strdata[-29:]
    #print("Index in string is",res)
    return res
    
# Get differences
def get_new_files(sftp,s3files):
    print("Getting new files....")
    list1 = sftp
    print("Lista 1 es",list1)
    list2 = s3files
    print("Lista 2 es",list2)
    
    #new_files = set(list1).difference(list2)
    new_files = [item for item in list1 if item not in list2]
    
    print("The difference is" , new_files)
    
    return new_files


def truncate_table():
    cursor = cnx.cursor()
    try:
        #cursor.execute(sql_truncate)
        cursor.execute("TRUNCATE TABLE sftp_hook")
        cnx.commit()
        print("Truncate table succesfully")
    except Exception as ex:
        print(ex)
        #messageDiscord("ETL ALTAN SFTP" , "Error running ALTAN SFTP ETL"+ ex)

# GET  WIWI S3 files    
def get_wiwi_s3_files():
    print("Getting WIWI S3 files....")
    s3_folder_data =subprocess.check_output(['aws','s3', 'ls', 's3://altan-data/sftp/','--human-readable'])
    im_data = s3_folder_data.decode("utf-8").splitlines()
    
    final_data = []
    for j in im_data:
    
        clean_file = j[31:]
        if clean_file.endswith('.7z'):
        #print("Archivo es limpio",clean_file)
            final_data.append(clean_file)
    
    #print("SFTP FILES :",type(final_data),final_data)
    #s3_list_data = list(s3_folder_data)
    return final_data

#Get Day before
def get_day_before():
    day_before = datetime.datetime.today() - datetime.timedelta(days=1)
    day_formatr = day_before.strftime("%Y%m%d")
    print(day_formatr)
    return(day_formatr)

if __name__ == "__main__":
    #do_sftp()
    #process_batch_dir()
    truncate_table()
    file = "EstadoConsumo_168_"+get_day_before()+".csv"
    print(file)
    process_file_forced([file])
    #get_day_before()
