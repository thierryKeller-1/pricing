
#__ Folder path contains data
BASE_DIR = {
    "booking": "",
    "maeva": "C:/Users/Keller/Documents/Jobdev/pricing/pricing/reconstruction_maeva/data/maeva",
    "edomizil": "",
    "campings": ""
}

# Folder to store missing data
OUTPUT_DIR = "C:/Users/Keller/Documents/Jobdev/pricing/pricing/reconstruction_maeva/data/recovery/missing"
#file to contains logs
LOG_FILE_DIR = "C:/Users/Keller/Documents/Jobdev/pricing/pricing/reconstruction_maeva/data/recovery/logs"

#__ Fileds that is required for each sites
FIELDS = {
    "booking": [

    ],
    "maeva": [
    "web-scrapper-order",
    "date_price",
    "date_debut",
    "date_fin",
    "prix_init",
    "prix_actuel",
    "typologie",
    "n_offre",
    "nom",
    "localite",
    "date_debut-jour",
    "Nb semaines",
    "cle_station",
    "nom_station"
    ],
    "campings": [

    ],
    "edomizil": [

    ]
}


#__ Fileds that is required for each sites
CHEKING_FIELDS = {
    "booking": [

    ],
    "maeva": [
    "typologie",
    "n_offre",
    "nom",
    "localite",
    "cle_station",
    "nom_station"
    ],
    "campings": [

    ],
    "edomizil": [

    ]
}