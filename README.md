# Rendu Analyse et visualisation de données
**Par Antoine LOIZEL, Younès GHENNAM, Mohamed Amin MALLEK & Roman RIEUNIER**

Ce projet est constitué de 3 fichiers permettant de lancer la récupération, le traitement et la persistance de données dans un cluster ElasticSearch.

1. `init.sh` un script bash qui télécharge et décompresse les données issues des stations de vélos de la ville de Parme
2. `bikes_preprocessing.py` un script python qui traite les données avec pandas et les sérialise dans des fichiers pickle `pkl` et `ndjson`
3. `bikes_persistance.py` un script python qui récupère les fichiers `ndjson` et les envoie à l'endpoint ElasticSearch défini dans le `.env`

## Marche à suivre
Cloner le repository et y placer à la racine le `.env` reçu par mail ou créer un `.env` suivant ce modèle :
```dotenv
ENDPOINT_URL=https://identifiant-du-cluster.localisation.aws.cloud.es.io:9243
AUTHENTICATION_TOKEN=Basic base64_encode("elastic:password")
```

Dans un terminal bash lancer les commandes suivantes :

```bash
./init.sh
python3 bikes_preprocessing.py
python3 bikes_persistance.py
```

Vous pouvez spécifier une taille de batch pour l'envoi à ElasticSearch, exemple pour envoyer les données par paquets de 5000 (défaut 10000) :

`python3 bikes_persistance.py 5000`

Vous pouvez ajouter les données à votre cluster Kibana depuis ElasticSearch et créer un index pattern, ou vous connecter à celui qui vous a été envoyé.
