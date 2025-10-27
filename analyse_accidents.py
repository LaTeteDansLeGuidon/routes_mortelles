# Code INSEE de la commune à analyser

# code_INSEE = 93051

import pandas as pd
import sqlite3
import requests
import streamlit as st

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

# Utilisation de session_state pour conserver les résultats d'analyse au téléchargement du csv
if 'rapport_part1' not in st.session_state:
    st.session_state.rapport_part1 = None
if 'rapport_tableau' not in st.session_state:
    st.session_state.rapport_tableau = None
if 'tableau_to_csv' not in st.session_state:
    st.session_state.tableau_to_csv = None


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

def extraire_accidents_par_date(code_insee,annee):
    conn = sqlite3.connect(f'accidents_{annee}.db')

    # Étape 1 : Extraire les véhicules impliqués dans chaque accident
    query_vehicules = f'''
    SELECT
        c.Num_Acc,
        GROUP_CONCAT(DISTINCT v.catv) AS vehicules_catv
    FROM vehicules v
    JOIN caract c ON v.Num_Acc = c.Num_Acc
    WHERE c.com = '{code_insee}'
    GROUP BY c.Num_Acc
    '''
    df_vehicules = pd.read_sql_query(query_vehicules, conn)

    # Étape 2 : Extraire les victimes (piétons et cyclistes)
    query_victimes = f'''
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
        c.Num_Acc,
        c.adr AS adresse,
        c.lat AS latitude,
        c.long AS longitude,
        u.id_usager AS id_victime
    FROM usagers u
    JOIN caract c ON u.Num_Acc = c.Num_Acc
    WHERE u.grav != 1 AND (u.catu = 3 OR (u.catu = 1 AND EXISTS (
        SELECT 1 FROM vehicules v
        WHERE v.Num_Acc = u.Num_Acc AND (v.catv = '01' OR v.catv = '80')
    ))) AND c.com = '{code_insee}'
    ORDER BY c.an, c.mois, c.jour, c.hrmn
    '''
    df_victimes = pd.read_sql_query(query_victimes, conn)


    # Étape 3 : Fusionner les données des victimes et des véhicules
    df = pd.merge(df_victimes, df_vehicules, on='Num_Acc', how='left')

    #print(df)

    # Étape 4 : Regrouper les codes catv selon catv_groupes
    def regrouper_vehicules(vehicules_catv_str):
        if pd.isna(vehicules_catv_str):
            return "Aucun véhicule"
        vehicules_catv = vehicules_catv_str.split(',')
        vehicules_groupes = set()
        for catv in vehicules_catv:
            catv_str = str(catv).zfill(2)
            groupe = catv_groupes.get(catv_str, 'Autres')
            vehicules_groupes.add(groupe)
        return ', '.join(sorted(vehicules_groupes))

    df['vehicules_impliques'] = df['vehicules_catv'].apply(regrouper_vehicules)

    # Formater la date en Python
    df['date_accident'] = df['annee'].astype(str) + '-' + df['mois'].astype(str).str.zfill(2) + '-' + df['jour'].astype(str).str.zfill(2)

    conn.close()
    return df





def analyser_accidents_commune(code_insee,annee):
    code_insee = str(code_insee)
    nom_commune = get_nom_commune(code_insee)
    conn = sqlite3.connect(f'accidents_{annee}.db')

    ANNEE_ACCIDENT = annee
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

    rapport = f"""
    # Analyse des accidents routiers {ANNEE_ACCIDENT} pour la commune de {nom_commune}
    
En {annee}, **{total_blesses_intro}** personnes à pied ou à vélo dont **{total_enfants_intro} enfants** ont été blessées ou tuées dans la ville\xa0:
    
🔵 **{pietons_blesses_intro} piétons** (dont **{pietons_enfants_intro} enfants**)
    
🔵 **{cyclistes_blesses_intro} cyclistes** (dont **{cyclistes_enfants_intro} enfants**)

## 🚶 Piétonnes et Piétons :
Parmi les **{pietons_blesses_intro}** piétonnes et piétons blessé·es ou tué·es dans des accidents, **{pietons.get('hospitalises', 0)}** ont été hospitalisé·es
"""
    # Ajout conditionnel des tués
    if pietons.get('tues', 0) > 0:
        rapport += f", et **{pietons.get('tues', 0)}** sont mort·es.\n\n"
    else:
        rapport += ".\n\n"

    rapport += "### Véhicules impliqués :\n\n"
    if vehicules_pietons_groupes.empty:
         rapport += "* Aucune collision avec des véhicules externes enregistrée.\n\n"
    for _, row in vehicules_pietons_groupes.iterrows():
        rapport += f"🔵 {row['nombre']} accidents impliquant un **{row['Mode_Transport']}**\n\n"

    rapport += f"""
## 🚴 **Cyclistes** :
Parmi les **{cyclistes_blesses_intro}** cyclistes blessé·es ou tué·es dans des accidents,  **{cyclistes.get('hospitalises', 0)}** ont été hospitalisé·es
"""

    # Ajout conditionnel des tués
    if cyclistes.get('tues', 0) > 0:
        rapport += f", et **{cyclistes.get('tues', 0)}** sont mort·es.\n\n"
    else:
        rapport += "\n\n"

    rapport += "### Véhicules impliqués :\n\n"
    if vehicules_cyclistes_groupes.empty:
        rapport += "- Aucune collision avec des véhicules externes enregistrée (victimes uniquement auto-accidentées ou indemnes).\n\n"
    else:
        for _, row in vehicules_cyclistes_groupes.iterrows():
            rapport += f"🔵 {row['nombre']} accidents impliquant un **{row['Mode_Transport']}**\n\n"

    # --- 6. Extraire et afficher les accidents par date ---
    df_accidents_par_date = extraire_accidents_par_date(code_insee,annee)
    df_accidents_par_date['gravite_libelle'] = df_accidents_par_date['gravite'].map(grav_dict)

    # # Ajouter le tableau des accidents à la fin du rapport
    # rapport += "\n\n"
    # rapport += "### 📅 Liste des victimes piéton·nes et cyclistes recensées dans la commune (triées par date) :\n"

    if not df_accidents_par_date.empty:
        # Sélectionner les colonnes à afficher
        tableau = df_accidents_par_date[['Num_Acc', 'date_accident', 'type_usager', 'gravite_libelle', 'vehicules_impliques', 'adresse', 'latitude', 'longitude']]
        tableau = tableau.rename(columns={
            'Num_Acc': 'ID Accident',
            'date_accident': 'Date',
            'type_usager': 'Type usager',
            'gravite_libelle': 'Gravité',
            'vehicules_impliques': 'Véhicules impliqués',
            'adresse' : 'Adresse',
            'latitude': 'Latitude',
            'longitude': 'Longitude'
        })
        # Sauvegarder une copie pour le CSV
        tableau_to_csv = tableau.copy()

        # Enlever latitude et longitude pour la visualisation web
        tableau.drop(axis = 1, columns = ['Latitude','Longitude'],inplace=True)
        
        # Ajouter le tableau au rapport sous forme de Markdown
        rapport_tableau = "\n\n" + tableau.to_markdown(index=False)
    else:
        rapport_tableau = "- Aucune victime recensée.\n"


    conn.close()
    return rapport,rapport_tableau, tableau_to_csv if not df_accidents_par_date.empty else None
# Exemple d'utilisation pour Noisy-le-Grand (code INSEE : 93051)
# code_INSEE = 93051
# annee = 2023
# print(analyser_accidents_commune(code_INSEE,annee))

# Interface Streamlit
st.set_page_config(
        page_title="Routes mortelles",
)
st.title("Analyse des accidents routiers par commune")
st.markdown(
    "Un outil créé par [LtdlGuidon](https://piaille.fr/@LTDLGuidon), pour analyser les données d'accidentologie, avec un focus sur les personnes à pied ou à vélo. Les données sont disponibles en opendata [sur datagouv](https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024/).\n\n" \
    "Le code est visible [sur Github](https://github.com/LaTeteDansLeGuidon/routes_mortelles). Il s'agit d'un travail amateur, des erreurs s'y glissent peut-être... N'hésitez pas à les signaler !\n\n"
    "Pour accéder aux données d'une commune, sélectionner une année, le **code INSEE** puis appuyer sur le bouton **valider**. Pour les communes ayant plusieurs codes INSEE comme Paris, les codes utilisés semblent être ceux des arrondissements. Attention, le code INSEE est différent du code postal !"
)
# Menu déroulant pour sélectionner l'année
annees_disponibles = list(range(2023, 2025))
index_par_defaut = annees_disponibles.index(annees_disponibles[-1])
annee = st.selectbox("Sélectionnez l'année", annees_disponibles,index=index_par_defaut)

# Champ de saisie pour le code INSEE
code_insee = st.text_input("**Code INSEE** de la commune (Par exemple pour Noisy-le-Grand : 93051)", "93051")

# Bouton pour lancer l'analyse
if st.button("Analyser"):
    if len(code_insee) == 5 and code_insee.isdigit():
        with st.spinner("Analyse en cours..."):
            st.session_state.rapport_part1, st.session_state.rapport_tableau, st.session_state.tableau_to_csv = analyser_accidents_commune(code_insee, annee)
    else:
        st.error("Le code INSEE doit être un nombre à 5 chiffres.")

# Affiche toujours le rapport s'il existe dans session_state
if st.session_state.rapport_part1 is not None:
    st.markdown(st.session_state.rapport_part1)
    # Titre pour la section du tableau
    st.markdown("### 📅 Liste des victimes piéton·nes et cyclistes recensées dans la commune (triées par date)")
    # Bouton de téléchargement (juste sous le titre)
    if st.session_state.tableau_to_csv is not None:
        csv = st.session_state.tableau_to_csv.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Télécharger le tableau en CSV",
            data=csv,
            file_name=f'accidents_{code_insee}_{annee}.csv',
            mime='text/csv',
        )
    # Afficher le tableau en Markdown (sans latitude/longitude)
    st.markdown(st.session_state.rapport_tableau)




