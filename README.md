#Brief 11 - Gestion des Offres d'Emploi

Description

Ce projet permet de gérer des offres d'emploi via une interface web. Il utilise Docker pour orchestrer les différents services, tels que PostgreSQL, pgAdmin, une API Flask, et une interface utilisateur Streamlit.

Le projet prend des données provenant d'un fichier CSV (rapport_travail.csv), crée une base de données PostgreSQL, puis y insère ces données sous forme de table des offres d'emploi. Il offre aussi une interface pour visualiser et interagir avec ces offres.

#Technologies utilisées

Docker : Conteneurisation de l'application.
PostgreSQL : Système de gestion de base de données.
pgAdmin : Interface graphique pour gérer PostgreSQL.
Flask : Framework Python pour créer l'API backend.
Streamlit : Framework Python pour créer l'interface frontend.
Pandas : Manipulation des données CSV pour l'insertion dans la base de données.
Prérequis

Avant de commencer, assurez-vous d'avoir installé les éléments suivants :
Docker et Docker Compose pour gérer les conteneurs.
Python 3.6+ et pip (pour installer les dépendances).
