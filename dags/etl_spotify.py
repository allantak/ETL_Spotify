# airflow-venv\Scripts\activate.bat
#.\airflow-venv\Scripts\Activate.ps1
# a  /c/Users/Usuario/airflow/dags
# /c/Users/Usuario/Desktop/Projetos/MyProjetos/Spotify
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


#Processo de TRANSFORM, limpagem de dados.
def check_data(df: pd.DataFrame) -> bool:

    if df.empty:
        print("As musicas não fizeram o download")
        return False 


    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Error Primary key")
    
    #Check for nulls
    if df.isnull().values.any():
        raise Exception("Tem valores nullos")




def run_spotify_etl():

    DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
    USER_ID = "Allan Takeuchi"
    TOKEN = "BQCEl7Tu2UQVzTCub8wN7DJEwmYVghpw8o-pAoHKMwAHP48mSQ3wZPLCim3RCgsjumKElmBgriIaabsCEZBPmPKhkj1tMogINvHSKcEDYNIKAaAbIb6hbw8E_x4HP6vf3hfScC6x45DelB9Oxln1Q8_idEaHxB08S_FK"
    #Inserir dados para poder fazer o request, padrao das Api's
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    #Vamos transformar o valor do dia anterior para milisegundo
    #Milisegundo, pois o request precisa desse informação
    hoje = datetime.datetime.now()
    ontem = hoje - datetime.timedelta(days=1)
    ontem_milisegundo = int(ontem.timestamp()) * 1000
    


    #Request das musicas que você esteve ouvindo ontem até o atual momento
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=ontem_milisegundo), headers=headers)
    data = r.json()



    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    
    
    
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])#Apenas a data

        
    
    song_dict = {
        "song_name":song_names,
        "artist_name":artist_names,
        "played_at":played_at_list,
        "timestamp":timestamps,
    }


    df_song = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    #TRANSFORM
    if check_data(df_song):
        print("Verificação dos dados")
        

    #LOAD dos dados
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()


    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """ 

    cursor.execute(sql_query)
    print('Abriu database com sucesso')

    try:
        df_song.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Dados inseridos no banco de dados")

    conn.close()
    print("Fechamento do database com sucesso")


    

    
