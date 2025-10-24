# Code INSEE de la commune à analyser

code_INSEE = 93051

import pandas as pd
import sqlite3
import requests

grav_dict = {1: 'Indemne', 2: 'Tué', 3: 'Blessé hospitalisé', 4: 'Blessé léger'}

# Dictionnaire de regroupement des catégories (catv)
catv_groupes = {
    # Automobilistes (VL, VU)
    '03': 'Automobiliste', '07': 'Automobiliste', '10': 'Automobiliste',

    # Poids-lourds (PL, Tracteurs)
    '13': 'Poids-lourd', '14': 'Poids-lourd', '15': 'Poids-lourd',
    '16': 'Poids-lourd', '17': 'Poids-lourd',

    # Vélo (y compris VAE)
    '01': 'Vélo (incl. VAE)', '80': 'Vélo (incl. VAE)',

    # 2RM (2 roues motorisées)
    '02': '2RM', '30': '2RM', '31': '2RM',
    '32': '2RM', '33': '2RM', '34': '2RM',

    # Bus/Cars
    '37': 'Bus/Car', '38': 'Bus/Car',

    # Autres
    '00': 'Autres', '04': 'Autres', '05': 'Autres', '06': 'Autres', '08': 'Autres', '09': 'Autres',
    '11': 'Autres', '12': 'Autres', '18': 'Autres', '19': 'Autres', '20': 'Autres', '21': 'Autres',
    '35': 'Autres', '36': 'Autres', '39': 'Autres', '40': 'Autres', '41': 'Autres', '42': 'Autres',
    '43': 'Autres', '50': 'Autres', '60': 'Autres', '99': 'Autres',
}

def grouper_catv(df):
    """Regroupe les catégories de véhicules d'un DataFrame selon le dictionnaire catv_groupes et trie par nombre décroissant."""
    # S'assurer que le DataFrame n'est pas vide avant de tenter le regroupement
    if df.empty:
        return pd.DataFrame({'Mode_Transport': [], 'nombre': []})
        
    df['catv_str'] = df['catv'].apply(lambda x: str(x).zfill(2))
    df['Mode_Transport'] = df['catv_str'].map(catv_groupes).fillna('Autres')
    df_groupes = df.groupby('Mode_Transport')['nombre'].sum().reset_index()
    return df_groupes.sort_values(by='nombre', ascending=False)

def get_nom_commune(code_insee):
    url = f"https://geo.api.gouv.fr/communes?code={code_insee}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data:
            return data[0]['nom']
    return "Nom non trouvé"

def extraire_accidents_par_date(code_insee):
    conn = sqlite3.connect('accidents_2024.db')
    query = f'''
    SELECT
        c.an AS annee,
        c.mois AS mois,
        c.jour AS jour,
        u.grav AS gravite,
        CASE
            WHEN u.catu = 3 THEN 'Piéton'
            WHEN u.catu = 1 THEN 'Cycliste'
            ELSE 'Autre'
        END AS type_usager,
        c.Num_Acc AS num_accident,
        c.lat AS latitude,
        c.long AS longitude,
        GROUP_CONCAT(DISTINCT v.catv) AS vehicules_impliques
    FROM usagers u
    JOIN caract c ON u.Num_Acc = c.Num_Acc
    LEFT JOIN vehicules v ON u.Num_Acc = v.Num_Acc
    WHERE u.grav != 1 AND (u.catu = 1 OR u.catu = 3) AND c.com = '{code_insee}'
    GROUP BY c.Num_Acc, c.an, c.mois, c.jour, u.grav, u.catu
    ORDER BY c.an, c.mois, c.jour, c.hrmn
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Formater la date en Python
    df['date_accident'] = df['annee'].astype(str) + '-' + df['mois'].astype(str).str.zfill(2) + '-' + df['jour'].astype(str).str.zfill(2)
    return df



def analyser_accidents_commune(code_insee):
    code_insee = str(code_insee)
    nom_commune = get_nom_commune(code_insee)
    conn = sqlite3.connect('accidents_2024.db')

    ANNEE_ACCIDENT = 2024
    AGE_MAX_ENFANT = 18
    ANNEE_MIN_ENFANT = ANNEE_ACCIDENT - AGE_MAX_ENFANT
    
    # Initialisation des résultats à zéro en cas d'absence de données
    default_stats = {'total_victimes': 0, 'tues': 0, 'hospitalises': 0, 'legers': 0}
    default_enfants = {'enfants_victimes': 0}

    # --- 1. Statistiques piétons (victimes graves/légères) ---
    query_pietons = f'''
    SELECT
        COUNT(*) AS total_victimes,
        SUM(CASE WHEN grav = 2 THEN 1 ELSE 0 END) AS tues,
        SUM(CASE WHEN grav = 3 THEN 1 ELSE 0 END) AS hospitalises,
        SUM(CASE WHEN grav = 4 THEN 1 ELSE 0 END) AS legers
    FROM usagers u
    JOIN caract c ON u.Num_Acc = c.Num_Acc
    WHERE u.grav !=1 AND u.catu = 3 AND c.com = '{code_insee}'
    '''
    df_pietons = pd.read_sql_query(query_pietons, conn)
    pietons = df_pietons.iloc[0] if not df_pietons.empty and df_pietons.iloc[0]['total_victimes'] > 0 else default_stats

    # --- 1.1. Enfants piétons blessés/tués ---
    query_pietons_enfants = f'''
    SELECT
        COUNT(*) AS enfants_victimes
    FROM usagers u
    JOIN caract c ON u.Num_Acc = c.Num_Acc
    WHERE u.grav !=1 AND u.catu = 3 AND c.com = '{code_insee}' AND u.an_nais > {ANNEE_MIN_ENFANT}
    '''
    df_pietons_enfants = pd.read_sql_query(query_pietons_enfants, conn)
    pietons_enfants = df_pietons_enfants.iloc[0] if not df_pietons_enfants.empty and df_pietons_enfants.iloc[0]['enfants_victimes'] > 0 else default_enfants


    # --- 2. Véhicules impliqués dans les accidents avec piétons ---
    query_vehicules_pietons = f'''
    SELECT v.catv, COUNT(*) AS nombre
    FROM usagers u
    JOIN caract c ON u.Num_Acc = c.Num_Acc
    JOIN vehicules v ON u.Num_Acc = v.Num_Acc
    WHERE u.grav != 1 AND u.catu = 3 AND c.com = '{code_insee}'
    GROUP BY v.catv
    '''
    vehicules_pietons = pd.read_sql_query(query_vehicules_pietons, conn)
    # Le regroupement gère maintenant les DataFrames vides
    vehicules_pietons_groupes = grouper_catv(vehicules_pietons)

    # --- 3. Statistiques cyclistes (victimes graves/légères) ---
    query_cyclistes = f'''
    SELECT
        COUNT(*) AS total_victimes,
        SUM(CASE WHEN grav = 2 THEN 1 ELSE 0 END) AS tues,
        SUM(CASE WHEN grav = 3 THEN 1 ELSE 0 END) AS hospitalises,
        SUM(CASE WHEN grav = 4 THEN 1 ELSE 0 END) AS legers
    FROM usagers u
    JOIN caract c ON u.Num_Acc = c.Num_Acc
    JOIN vehicules v ON u.Num_Acc = v.Num_Acc AND u.num_veh = v.num_veh
    WHERE u.grav !=1 AND u.catu = 1 AND (v.catv = '01' OR v.catv = '80') AND c.com = '{code_insee}'
    '''
    df_cyclistes = pd.read_sql_query(query_cyclistes, conn)
    cyclistes = df_cyclistes.iloc[0] if not df_cyclistes.empty and df_cyclistes.iloc[0]['total_victimes'] > 0 else default_stats
    
    # --- 3.1. Enfants cyclistes blessés/tués ---
    query_cyclistes_enfants = f'''
    SELECT
        COUNT(*) AS enfants_victimes
    FROM usagers u
    JOIN caract c ON u.Num_Acc = c.Num_Acc
    JOIN vehicules v ON u.Num_Acc = v.Num_Acc AND u.num_veh = v.num_veh
    WHERE u.grav !=1 AND u.catu = 1 AND (v.catv = '01' OR v.catv = '80') AND c.com = '{code_insee}' AND u.an_nais > {ANNEE_MIN_ENFANT}
    '''
    df_cyclistes_enfants = pd.read_sql_query(query_cyclistes_enfants, conn)
    cyclistes_enfants = df_cyclistes_enfants.iloc[0] if not df_cyclistes_enfants.empty and df_cyclistes_enfants.iloc[0]['enfants_victimes'] > 0 else default_enfants

    # --- 4. Véhicules impliqués dans les accidents avec cyclistes ---
    query_vehicules_cyclistes = f'''
    SELECT v.catv, COUNT(*) AS nombre
    FROM usagers u
    JOIN caract c ON u.Num_Acc = c.Num_Acc
    JOIN vehicules v ON u.Num_Acc = v.Num_Acc
    JOIN vehicules v_cycliste ON u.Num_Acc = v_cycliste.Num_Acc AND u.num_veh = v_cycliste.num_veh
    WHERE u.grav !=1 AND u.catu = 1 AND (v_cycliste.catv = '01' OR v_cycliste.catv = '80') AND c.com = '{code_insee}' AND v.num_veh != v_cycliste.num_veh
    GROUP BY v.catv
    '''
    vehicules_cyclistes = pd.read_sql_query(query_vehicules_cyclistes, conn)
    # Le regroupement gère maintenant les DataFrames vides
    vehicules_cyclistes_groupes = grouper_catv(vehicules_cyclistes)

    # --- 5. Générer le rapport ---

    # Calcul des totaux pour l'introduction
    # Utilisation de .get() pour s'assurer qu'une valeur 0 est retournée même si la série est mal formée
    pietons_blesses_intro = pietons.get('total_victimes', 0)
    pietons_enfants_intro = pietons_enfants.get('enfants_victimes', 0)
    cyclistes_blesses_intro = cyclistes.get('total_victimes', 0)
    cyclistes_enfants_intro = cyclistes_enfants.get('enfants_victimes', 0)
    
    total_blesses_intro = pietons_blesses_intro + cyclistes_blesses_intro
    total_enfants_intro = pietons_enfants_intro + cyclistes_enfants_intro

    # Création du paragraphe d'introduction
    intro_paragraph = f"""L'année dernière, **{total_blesses_intro}** personnes dont **{total_enfants_intro} enfants** ont été blessées (ou tuées) dans des accidents de piétons ou cyclistes dans la ville :\n
    🔵 **{pietons_blesses_intro} piétons** (dont **{pietons_enfants_intro} enfants**)\n
    🔵 **{cyclistes_blesses_intro} cyclistes** (dont **{cyclistes_enfants_intro} enfants**)
    """

    rapport = f"""
    Analyse des accidents routiers {ANNEE_ACCIDENT} pour la commune {nom_commune} :
    
{intro_paragraph}

    ***

    🚶 **Piétonnes et Piétons** :
    Sur les **{pietons_blesses_intro}** piétonnes et piétons victimes (blessé·es ou tué·es), **{pietons.get('hospitalises', 0)}** ont été hospitalisé·es.
    """
    # Ajout conditionnel des tués
    if pietons.get('tues', 0) > 0:
        rapport += f"et **{pietons.get('tues', 0)}** sont mort·es.\n"
    else:
        rapport += "\n"

    rapport += "\n    Modes de transport impliqués :\n"
    if vehicules_pietons_groupes.empty:
         rapport += "    * Aucune collision avec des véhicules externes enregistrée.\n"
    for _, row in vehicules_pietons_groupes.iterrows():
        rapport += f"    🔵 {row['nombre']} accidents impliquant un **{row['Mode_Transport']}**\n"

    rapport += f"""
    ***

    🚴 **Cyclistes** :
    Sur les **{cyclistes_blesses_intro}** cyclistes victimes (blessé·es ou tué·es), **{cyclistes.get('hospitalises', 0)}** ont été hospitalisé·es.
    """
    # Ajout conditionnel des tués
    if cyclistes.get('tues', 0) > 0:
        rapport += f"et **{cyclistes.get('tues', 0)}** sont mort·es.\n"
    else:
        rapport += "\n"

    rapport += "\n    Modes de transport impliqués :\n"
    if vehicules_cyclistes_groupes.empty:
         rapport += "    * Aucune collision avec des véhicules externes enregistrée (victimes uniquement auto-accidentées ou indemnes).\n"
    for _, row in vehicules_cyclistes_groupes.iterrows():
        rapport += f"    🔵 {row['nombre']} accidents impliquant un **{row['Mode_Transport']}**\n"

    # --- 6. Extraire et afficher les accidents par date ---
    df_accidents_par_date = extraire_accidents_par_date(code_insee)
    df_accidents_par_date['gravite_libelle'] = df_accidents_par_date['gravite'].map(grav_dict)

    # Ajouter le tableau des accidents à la fin du rapport
    rapport += "\n\n"
    rapport += "📅 **Liste des accidents recensés (triés par date) :**\n"
    if not df_accidents_par_date.empty:
        # Sélectionner les colonnes à afficher
        tableau = df_accidents_par_date[['num_accident', 'date_accident', 'type_usager', 'gravite_libelle', 'vehicules_impliques', 'latitude', 'longitude']]
        tableau = tableau.rename(columns={
            'num_accident': 'ID Accident',
            'date_accident': 'Date',
            'type_usager': 'Type usager',
            'gravite_libelle': 'Gravité',
            'vehicules_impliques': 'Véhicules impliqués',
            'latitude': 'Latitude',
            'longitude': 'Longitude'
        })
        # Ajouter le tableau au rapport sous forme de chaîne
        rapport += tableau.to_string(index=False)
    else:
        rapport += "* Aucun accident recensé.\n"

    conn.close()
    return rapport

# Exemple d'utilisation pour Noisy-le-Grand (code INSEE : 93051)

print(analyser_accidents_commune(code_INSEE))

