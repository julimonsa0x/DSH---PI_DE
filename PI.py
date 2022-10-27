import os
from time import sleep
import sqlite3
import pandas as pd


# Funciones utiles para el proyecto
def creacion_database(guardar:bool=True):
    """Creacion de la base de datos

    Parametro:
    guardar (bool): Booleano para guardar o no la BBDD.
    En caso de no querer guardarla, la BBDD correra en memoria (sqlite feature).\n
    La idea es guardarla en el workspace para poder hacer query en cualquier momento
    desde DB Browser for SQLite (programa externo) o desde aca mismo.
    """

    global conn
    global cur

    # Creamos el archivo .db
    try:
        # si existe, nos conectamos
        if os.path.exists('pi_database.db'):
            if not guardar:
                conn = sqlite3.connect(':memory:')
            conn = sqlite3.connect('pi_database.db')
            cur = conn.cursor()
            print("[DEBUG] Conectado a la base de datos exitosamente")
            sleep(1)
        
        # si no existe, se crea y se conecta.
        else:
            with open('pi_database.db', 'w', encoding="utf8"):
                if not guardar:
                    conn = sqlite3.connect(':memory:')
                conn = sqlite3.connect('pi_database.db')
                cur = conn.cursor()
                print("[DEBUG] Se creo la Base de datos exitosamente")
                sleep(1)

    except:
        try:
            # En el peor caso, usar el absolute path
            FULL_PATH = str(os.curdir)
            conn = sqlite3.connect(FULL_PATH)
            cur = conn.cursor()
            print("[DEBUG] Conexion mediante absolute path")
        except Exception:
            print('[DEBUG] Todo lo que pudo fallar, falló ¯\_(ツ)_/¯')
    sleep(1)
    
    # Creamos las tablas
    cur.execute("DROP TABLE IF EXISTS henry_pi01;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS henry_pi01 (
        precio REAL,
        producto_id TEXT,
        sucursal_id TEXT );
        """
    )
    conn.commit()
    print("[DEBUG] Base de datos creada con exito lista para ingesta de datos")


def permutar_columnas(df, col1, col2):
    """Permutacion de columnas para el archivo excel que tiene las columnas cambiadas de orden"""
    col_list = list(df.columns)
    x, y = col_list.index(col1), col_list.index(col2)
    col_list[y], col_list[x] = col_list[x], col_list[y]
    df = df[col_list]
    return df


################################################################

## Cargo la data desde Raw_Data
archivos_fuente = sorted(os.listdir(path='Raw_Data'))
#print(archivos_fuente)


precios_semana_1 = pd.read_csv(f'Raw_Data/{archivos_fuente[0]}', encoding='utf-16')
precios_semana_2 = pd.read_json(f'Raw_Data/{archivos_fuente[1]}')
precios_semana_3 = pd.read_csv(f'Raw_Data/{archivos_fuente[2]}', delimiter='|')

pr_week_4 = pd.read_excel(f'Raw_Data/{archivos_fuente[3]}', sheet_name=None, engine='openpyxl')
precios_semana_4 = pd.concat(pr_week_4, ignore_index=True)

producto = pd.read_parquet(f'Raw_Data/{archivos_fuente[4]}')
sucursal = pd.read_csv(f'Raw_Data/{archivos_fuente[5]}')


## Normalizo la data cargada
def normalizar_csv(df: pd.DataFrame) -> pd.DataFrame:
    # Los primeros 3 archivos sufriran las siguientes modificaciones:
    # - Quedarnos unicamente con los ultimos 13 caracteres correspondientes al codigo EAN-13 de producto_id
    # - Dejaremos la columna precio en float64 mediante infer_objects
    # - sucursal_id no se vera alterada
    df.producto_id = df.producto_id.apply(lambda x: str(x)[-13:].zfill(13))
    df = df.convert_dtypes(infer_objects=True)
    return df

def normalizar_xlsx(df: pd.DataFrame) -> pd.DataFrame:
    # Este archivo sufrira los siguientes cambios:
    # - Se permutaran las columnas sucursal_id y producto_id para mantener el orden
    # - se eliminara el 00:00:00 de sucursal_id
    permutar_columnas(df, 'sucursal_id', 'producto_id')
    df.producto_id = df.producto_id.apply(lambda x: str(x).rstrip('.0'))
    df.sucursal_id = df.sucursal_id.apply(lambda x: str(x).split()[0])
    df = df.convert_dtypes(infer_objects=True)
    return df

def normalizar_producto(df: pd.DataFrame) -> pd.DataFrame:
    # Este archivo sufrira las siguientes modificaciones:
    # - columna id renombrada a producto_id
    # - los valores faltantes en las ultimas 3 categorias seran rellenados por 'sin categoria'  <TEXT>
    # - la columna nombre perdera las ultimas dos palabras para no ser redundante con la columna posterior
    df.rename(columns = {'id':'producto_id'}, inplace = True)
    df.fillna({'categoria1':'sin categoria','categoria2':'sin categoria','categoria3':'sin categoria'}, inplace=True)
    df.nombre = df.nombre.apply(lambda x: str(' '.join(i for i in str(x).split()[:-2])))
    df = df.convert_dtypes(infer_objects=True)
    return df

def normalizar_sucursal(df: pd.DataFrame) -> pd.DataFrame:
    # Este archivo sufrira los siguientes cambios:
    # - Columna id renombrada a sucursal_id
    df.rename(columns = {'id':'sucursal_id'}, inplace = True)
    return df


df1 = normalizar_csv(precios_semana_1)
df2 = normalizar_csv(precios_semana_2)
df3 = normalizar_csv(precios_semana_3)

df4 = normalizar_xlsx(precios_semana_4)

df5 = normalizar_producto(producto)
df6 = normalizar_sucursal(sucursal)


## Concateno todos los precios_semana en un unico .csv
# El nuevo .csv precios_semana_full contendra a 413, 503, 518 y las dos hojas del excel
# parte del proceso excluye el dropeo de duplicados aunque haciendolo se ahorraban unos 10mb de contenido
# pasando de ~67 mb a unos 50 mb

precios_semana_final = pd.concat([df1, df2, df3, df4])

## Mando la data en .csv a Processed_Data
precios_semana_final.to_csv(f'Processed_Data/precios_semana_full.csv', index=False)
producto.to_csv(f'Processed_Data/{archivos_fuente[4][:-8]}.csv', index=False)
sucursal.to_csv(f'Processed_Data/{archivos_fuente[5][:-4]}.csv', index=False)

def check_and_load_files():
    
    for i in archivos_fuente:
        print(i)
    if len(archivos_fuente) != 6:
        print('[DEBUG]Se encontraron nuevos archivos') 
    
    # to-do: Implementar normalizacion de nuevos datos

    archivos_norm = os.listdir(path='Processed_Data')
    if len(archivos_norm) != 0:
        print('[DEBUG]Ya existen los archivos normalizados')
    else:
        print('[DEBUG]No se encontraron archivos normalizados')

#check_and_load_files()
creacion_database()
precios_semana_final.to_sql('henry_pi01', conn, if_exists='replace')


# query final
print(cur.execute("SELECT AVG(precio) FROM henry_pi01 WHERE sucursal_id LIKE '%9-1-688%'").fetchall())


################################################################