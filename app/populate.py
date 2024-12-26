import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Text, Date, Float, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

##########################################################################################################################
host = 'postgres_container'
database = 'rapport_travail'
user = 'admin'
password = 'password'
port = '5432'
##########################################################################################################################

#String de connection
DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"

#Engine de la database
engine = create_engine(DATABASE_URL)
Base = declarative_base()

#Schema de la table
class JobOffer(Base):
    __tablename__ = 'job_offers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_offer = Column(String(255))
    profession = Column(String(255))
    description = Column(Text)
    date_de_creation = Column(Date)
    latitude = Column(Float)
    longitude = Column(Float)
    type_de_contrat = Column(String(255))
    experience_exige = Column(String(255))
    alternance = Column(Boolean)
    metier = Column(String(255))
    lieu_de_travail = Column(String(255))

#Creation de la table dans la database
Base.metadata.create_all(engine)

#Lecture du csv
df = pd.read_csv("rapport_travail.csv", delimiter=',', encoding='utf-8')

#Creation  de la session
Session = sessionmaker(bind=engine)
session = Session()

#Insertion des données dans la table table
for _, row in df.iterrows():
    job_offer = JobOffer(
        id_offer=row['id'],
        profession=row['profession'],
        description=row['description'],
        date_de_creation=pd.to_datetime(row['date_de_creation']),
        latitude=row['latitude'],
        longitude=row['longitude'],
        type_de_contrat=row['type_de_contrat'],
        experience_exige=row['experience_exige'],
        alternance=row['alternance'].lower() == 'true',
        metier=row['metier'],
        lieu_de_travail=row['lieu_de_travail']
    )
    session.add(job_offer)

#Commit de la session
session.commit()

print("Data inserted")

#Fermeture de la session
session.close()
