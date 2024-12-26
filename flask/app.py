from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from offres_emploi import Api
from offres_emploi.utils import dt_to_str_iso
from copy import copy
import time
from collections.abc import MutableMapping
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Sequence, DateTime, Float, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy_utils import database_exists, create_database
import logging
from collections.abc import MutableMapping

app = Flask(__name__)

app.logger.setLevel(logging.DEBUG)

app.logger.info("Flask app initialized successfully!")

client_id = ""
client_secret = ""

client = Api(client_id=client_id, 
             client_secret=client_secret)

try:
    response = client.search() 
    print("API client connected successfully:", response)
except Exception as e:
    print("API client connection failed:", str(e))

def dt_to_str_iso(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

start = datetime(2024, 12, 3)  # Example start date
end = datetime(2024, 12, 5)   # Example end date
delta = timedelta(days=15)     # Collect data per day
all_results = []

def collect_data(start, end, delta, all_results):
    local_start = copy(start)
    while local_start < end:
        time.sleep(2)
        local_end = local_start + delta
        print(f"Start getting data from {local_start} to {local_end}")
        results = []

        Rome = ['E1205','E1104','M1801','M1802','M1803','M1805','M1810','M1815','M1820','M1830','E1206','M1811','M1813'] 
        
        for code_rome in Rome:
            time.sleep(2)
            params = {
                "codeROME": code_rome,
                'minCreationDate': dt_to_str_iso(local_start),
                'maxCreationDate': dt_to_str_iso(local_end)
            }
            try:
                print(f"Requesting data for codeROME: {code_rome} from {local_start} to {local_end}")
                response = client.search(params=params)
                num_results = int(response["Content-Range"]["max_results"])
                results = response["resultats"]
            except AttributeError:
                print(f"No results for {code_rome} from {local_start} to {local_end}. Continue...")
                num_results = 0
            except Exception as e:
                print("Error!")
                print(e)
                print(type(e))
                num_results = 0

            if num_results > 149:
                print(f"Too much results: {num_results}")
                collect_data(local_start, local_end, delta / 2, all_results)
            else:
                print(f"{num_results} results collected.")

            all_results += results
            local_start += delta
            print(f"Moving to next date range: {local_start} to {local_start + delta}")

@app.route("/", methods=["GET"])
def hello_world():
    return "Hello, World!"

@app.route("/collect", methods=["POST"])
def get_datetime():
    data = request.get_json()

    if "begin_datetime" not in data:
        return jsonify({'error': 'No datetime provided'}), 400

    try:
        dt = datetime.fromisoformat(data['begin_datetime'])
        dt += timedelta(seconds=1)
    except ValueError:
        return jsonify({'error': 'Invalid datetime format'}), 400        

    collect_data(start, end, delta, all_results)

    def flatten(dictionary, parent_key='', separator='_'):
        items = []
        for key, value in dictionary.items():
            new_key = parent_key + separator + key if parent_key else key
            if isinstance(value, MutableMapping):
                items.extend(flatten(value, new_key, separator=separator).items())
            else:
                items.append((new_key, value))
        return dict(items)

    all_results_flatten = [flatten(results) for results in all_results]

    results_df = pd.DataFrame(all_results_flatten)


    df = results_df 

    df["metier"] = df["romeCode"] + " " + df["romeLibelle"]

    df["lieu_de_travail"] = df["lieuTravail_libelle"] + " " + df["lieuTravail_codePostal"].astype(str)

    df.drop(['agence_telephone','conditionExercice','contact_telephone','entreprise_url','offresManqueCandidats',
    'contact_courriel',
    'complementExercice',            
    'experienceCommentaire',         
    'entreprise_logo',                 
    'langues',                 
    'contact_urlPostulation',
    'nombrePostes',                         
    'accessibleTH',                         
    'origineOffre_origine',                
    'origineOffre_urlOrigine',       
    'origineOffre_partenaires',       
    'entreprise_description',          
    'dureeTravailLibelle',             
    'dureeTravailLibelleConverti',     
    'codeNAF',                         
    'secteurActivite',                
    'secteurActiviteLibelle',         
    'salaire_libelle',                
    'salaire_commentaire',            
    'qualificationCode',             
    'qualificationLibelle',            
    'permis',                          
    'competences',                     
    'salaire_complement1',             
    'salaire_complement2',             
    'qualitesProfessionnelles',       
    'formations',                      
    'contact_nom',                     
    'contact_coordonnees1',            
    'contact_coordonnees2',            
    'contact_coordonnees3',
    'dateActualisation',
    'agence_courriel',
    'deplacementCode',
    'deplacementLibelle',
    'natureContrat',
    'experienceLibelle',
    'entreprise_entrepriseAdaptee',
    'appellationlibelle',
    'entreprise_nom',
    'typeContratLibelle',
    'lieuTravail_libelle',
    'romeCode',
    'lieuTravail_commune',
    'lieuTravail_codePostal',
    'romeLibelle'
    ],axis=1, inplace = True)

    df.rename(index=str, columns={"id": "id",
                                "intitule": "profession",
                                "description": "description",
                                "dateCreation": "date_de_creation",
                                "lieuTravail_latitude":"latitude",
                                "lieuTravail_longitude":"longitude",
                                "rom":"metier",
                                "typeContrat": "type_de_contrat",
                                "alternance": "alternance",
                                "experienceExige":"experience_exige"
                                }, inplace=True)

    df.dropna(axis=0,inplace = True) 

    tech_rome_codes = ['E1205 Graphiste', 
                        'E1104 Concepteur / Conceptrice de contenus multimedia',
                        'M1801 Administrateur / Administratrice réseau informatique', 
                        'M1802 Ingénieur / Ingénieure système informatique' ,
                        'M1803 Directeur / Directrice des services informatiques',
                        'M1805 Développeur / Développeuse web',
                        'M1810 Technicien / Technicienne informatique',
                        'M1815 Développeur / Développeuse web',
                        'M1820 Ingénieur / Ingénieure système informatique',
                        'M1830 Administrateur / Administratrice réseau informatique',
                        'E1206 UX - UI Designer',
                        'M1811 Data engineer',
                        'M1813 Intégrateur / Intégratrice logiciels métiers'
                        ] 


    tech_filter = df["metier"].isin(tech_rome_codes)

    df_tech = df.loc[tech_filter,:]

    ##########################################################################################################################
    host='postgres_container'  #pgAdmin -> Servers -> name_server right click -> Properties -> Connection -> Host name/adress
    database='rapport_travail'  #pgAdmin database name
    user='admin'  # docker-compose.yml -> sevices -> postgres -> enviroment -> POSTGRES_USER
    password='password'  # docker-compose.yml -> sevices -> postgres -> enviroment -> POSTGRES_PASSWOED
    port='5432' #pgAdmin -> Servers -> name_server right click -> Properties -> Connection -> Port
    ##########################################################################################################################

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    engine = create_engine(connection_string)

    try:
    
        with engine.connect() as connection:
            print("Connected to the database successfully!")

        if_exists = "append"  #"replace"
        df_tech.to_sql(name="job_offers", con=engine, if_exists=if_exists, index=False)

        print(df_tech.shape)
    except Exception as e:
        print(f"Error connecting to the database: {e}")

    return jsonify({'data': all_results}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)