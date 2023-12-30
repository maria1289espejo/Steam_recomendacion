# Librerias a utilizar
from fastapi import FastAPI, HTTPException
from typing import List, Dict
import pandas as pd
from datetime import datetime
from fastparquet import ParquetFile
from unidecode import unidecode

# Pone nombre, descripción y versión a la API
app = FastAPI(title='Juegos recomendadas',
              description='Sistema de recomendación de juegos de la plataforma Steam basado en tus gustos',
              version='1.0')

# Cargar los archivos para realizar consultas
data_reviews = ParquetFile('datasets_limpios/df_reviews_sentiment.parquet.gzip')
df_data_reviews = data_reviews.to_pandas()
data_items = ParquetFile('datasets_limpios/df_items.parquet')
df_data_items = data_items.to_pandas()
data_games = ParquetFile('datasets_limpios/df_games.parquet')
df_data_games = data_games.to_pandas()

# End ponit de prueba
@app.get("/")
def read_root():
    return {"Bienvenido al sistema de recomendación de juegos"}

@app.get("/users_recommend/{año}")
def users_recommend(año: int):
    '''Se ingresa el año de interes y devulve el top 3 de los juegos más recomendados por usuario con el respectivo puntaje de analisis de sentimiento'''
    
    # Verificar si el input es un entero
    try:
        numero_entero = int(año)        

        # Verificar si el entero está presente en la columna 'posted' del DataFrame
        if numero_entero in (pd.to_datetime(df_data_reviews['posted'], yearfirst=True).dt.year).values:
            # Filtrar el DataFrame por el año ingresado
            df_año = df_data_reviews[pd.to_datetime(df_data_reviews['posted'], yearfirst=True).dt.year == numero_entero]

            # Contar las ocurrencias de True en la columna 'recommend' para cada 'item_id'
            conteo_recomendados = df_año.groupby('item_id')['recommend'].apply(lambda x: (x == 'True').sum()).reset_index(name='conteo_recomendados')

            # Obtener los 3 item_id con mayor cantidad de True
            top3_recomendados = conteo_recomendados.sort_values('conteo_recomendados', ascending=False).head(3)


            # Agregar la columna 'item_name' del df_data_items al top3_recomendados usando apply y lambda
            top3_recomendados['item_name'] = top3_recomendados['item_id'].apply(lambda x: df_data_games[df_data_games['id'] == x]['title'].iloc[0] if not df_data_games[df_data_games['id'] == x].empty else None)

            # Agregar la columna 'sentiment_analysis_posotivo/neutral' que cuenta las ocurrencias de 2 y 1 en 'sentiment_analysis'
            top3_recomendados['sentiment_analysis_posotivo/neutral'] = top3_recomendados['item_id'].apply(lambda x: df_año[df_año['item_id'] == x]['sentiment_analysis'].isin([2, 1]).sum())

            # Mostrar el resultado
            respuesta = top3_recomendados
        else:
            respuesta = print(f"{numero_entero} no está disponible, intenta con otro año.")

    except ValueError:
        respuesta = print(año, "no es un año, intenta de nuevo.")
    return respuesta

@app.get("/users_not_recommend/{año}")
def users_not_recommend(año: int):
    '''Se ingresa el año de interes y devulve el top 3 de los juegos menos recomendados por usuario con el respectivo puntaje de analisis de sentimiento'''
    # Verificar si el input es un entero
    try:
        numero_entero = int(año)

        # Verificar si el entero está presente en la columna 'posted' del DataFrame
        if numero_entero in (pd.to_datetime(df_data_reviews['posted'], yearfirst=True).dt.year).values:
            # Filtrar el DataFrame por el año ingresado
            df_año = df_data_reviews[pd.to_datetime(df_data_reviews['posted'], yearfirst=True).dt.year == numero_entero]

            # Contar las ocurrencias de False en la columna 'recommend' para cada 'item_id'
            conteo_no_recomendados = df_año.groupby('item_id')['recommend'].apply(lambda x: (x == 'False').sum()).reset_index(name='conteo_no_recomendados')


            # Obtener los 3 item_id con mayor cantidad de False
            top3_no_recomendados = conteo_no_recomendados.sort_values('conteo_no_recomendados', ascending=False).head(3)


            # Agregar la columna 'item_name' del df_data_items al top3_recomendados usando apply y lambda
            top3_no_recomendados['item_name'] = top3_no_recomendados['item_id'].apply(lambda x: df_data_games[df_data_games['id'] == x]['title'].iloc[0] if not df_data_games[df_data_games['id'] == x].empty else None)

            # Agregar la columna 'sentiment_analysis_negativo' que cuenta las ocurrencias de 0 en 'sentiment_analysis'
            top3_no_recomendados['sentiment_analysis_negativo'] = top3_no_recomendados['item_id'].apply(lambda x: df_año[df_año['item_id'] == x]['sentiment_analysis'].isin([0]).sum())

            # Mostrar el resultado
            respuesta = top3_no_recomendados
        else:
            respuesta = print(f"{numero_entero} no está disponible, intenta con otro año.")

    except ValueError:
        respuesta = print(año, "no es un año, intenta de nuevo.")
    return respuesta