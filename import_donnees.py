import pandas as pd
import sqlite3

for annee in range(2020, 2025):
    try:
        # 1. Lire les fichiers CSV avec l'année dynamique
        caract = pd.read_csv(f'caract-{annee}.csv', sep=';', encoding='utf-8')
        lieux = pd.read_csv(f'lieux-{annee}.csv', sep=';', encoding='utf-8')
        usagers = pd.read_csv(f'usagers-{annee}.csv', sep=';', encoding='utf-8')
        vehicules = pd.read_csv(f'vehicules-{annee}.csv', sep=';', encoding='utf-8')

        # 2. Connexion à la base SQLite avec un nom dynamique
        db_name = f'accidents_{annee}.db'
        conn = sqlite3.connect(db_name)

        # 3. Écrire chaque DataFrame dans une table SQLite
        caract.to_sql('caract', conn, if_exists='replace', index=False)
        lieux.to_sql('lieux', conn, if_exists='replace', index=False)
        usagers.to_sql('usagers', conn, if_exists='replace', index=False)
        vehicules.to_sql('vehicules', conn, if_exists='replace', index=False)

        print(f"Données pour l'année {annee} importées avec succès dans {db_name}")

    except FileNotFoundError as e:
        print(f"Erreur: Fichier non trouvé pour l'année {annee}: {e}")
    except Exception as e:
        print(f"Erreur lors du traitement de l'année {annee}: {e}")
    finally:
        # 4. Fermer la connexion si elle existe
        if 'conn' in locals():
            conn.close()

