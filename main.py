
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

#Process single file
def process_file_forced(files):
    print("Reading CSV :",files)
    
    #sql = "INSERT INTO seq(fecha_hora,msidn,imei,imsi,app_user,consumo_app,marca_handset,modelo_handset,red_conn_off,ubicacion)"
    cursor = cnx.cursor()
    
    for file in files:
        
         sql_load_s3 ="LOAD DATA FROM S3 's3://altan-data/sftp/%s' INTO TABLE sftp_hook_prod FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (fecha_trans,fecha_medicion,cliente, fecha_inicio_pf, fecha_fin_pf,msisdn, offer_id, offer_name, free_unit_id, \
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
