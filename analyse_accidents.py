import streamlit as st
import pandas as pd
import sqlite3
import requests

# Charger le fichier des communes de l'INSEE
@st.cache_data
def load_communes():
    df = pd.read_csv('v_commune_2024.csv', sep=',', dtype=str, encoding='utf-8', quotechar='"')
    return df

# Charger les données
communes_df = load_communes()

# Création d'une liste, sous la forme : Ville (00) - INSEE : 00000
communes_df['nom_et_insee'] = communes_df['LIBELLE'] + " (" + communes_df['DEP'] + ") - INSEE : " + communes_df['COM']

# Déplacer la colonne en première position
communes_df.insert(0, 'nom_et_insee', communes_df.pop('nom_et_insee'))

# Dictionnaires pour l'analyse
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
    '02': '2RM', '30': '2RM', '31': '2RM', '32': '2RM', '33': '2RM', '34': '2RM',
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
# Initialisation de l'état du tableau détaillé
if 'show_tableau' not in st.session_state:
    st.session_state.show_tableau = False

# Fonction pour regrouper les catégories de véhicules
def grouper_catv(df):
    # Regroupe les catégories de véhicules d'un DataFrame selon le dictionnaire catv_groupes et trie par nombre décroissant.
    # S'assurer que le DataFrame n'est pas vide avant de tenter le regroupement
    if df.empty:
        return pd.DataFrame({'Mode_Transport': [], 'nombre': []})
        
    df['catv_str'] = df['catv'].apply(lambda x: str(x).zfill(2))
    df['Mode_Transport'] = df['catv_str'].map(catv_groupes).fillna('Autres')
    df_groupes = df.groupby('Mode_Transport')['nombre'].sum().reset_index()
    return df_groupes.sort_values(by='nombre', ascending=False)


# Fonction pour extraire les accidents par date
def extraire_accidents_par_date(codes_insee, annee):
    df_accidents_par_date_consolide = pd.DataFrame()
    for code_insee in codes_insee:
        conn = sqlite3.connect(f'accidents_{annee}.db')
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
        df = pd.merge(df_victimes, df_vehicules, on='Num_Acc', how='left')
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
        df['date_accident'] = df['annee'].astype(str) + '-' + df['mois'].astype(str).str.zfill(2) + '-' + df['jour'].astype(str).str.zfill(2)
        df_accidents_par_date_consolide = pd.concat([df_accidents_par_date_consolide, df], ignore_index=True)
        conn.close()
    return df_accidents_par_date_consolide

# Fonction pour analyser les accidents d'une commune
def analyser_accidents_commune(nom_commune,codes_insee, annee):
    df_pietons_consolide = pd.DataFrame()
    df_cyclistes_consolide = pd.DataFrame()
    vehicules_pietons_groupes_consolide = pd.DataFrame()
    vehicules_cyclistes_groupes_consolide = pd.DataFrame()
    total_victimes = 0
    total_tues = 0
    total_hospitalises = 0
    total_legers = 0
    total_enfants_victimes = 0
    total_enfants_victimes_cyclistes = 0
    total_enfants_victimes_pietons = 0
    ANNEE_ACCIDENT = annee
    AGE_MAX_ENFANT = 18
    ANNEE_MIN_ENFANT = ANNEE_ACCIDENT - AGE_MAX_ENFANT
    for code_insee in codes_insee:
        code_insee = str(code_insee)
        conn = sqlite3.connect(f'accidents_{annee}.db')
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
        if not df_pietons.empty:
            df_pietons_consolide = pd.concat([df_pietons_consolide, df_pietons], ignore_index=True)
        query_pietons_enfants = f'''
        SELECT
            COUNT(*) AS enfants_victimes
        FROM usagers u
        JOIN caract c ON u.Num_Acc = c.Num_Acc
        WHERE u.grav !=1 AND u.catu = 3 AND c.com = '{code_insee}' AND u.an_nais > {ANNEE_MIN_ENFANT}
        '''
        df_pietons_enfants = pd.read_sql_query(query_pietons_enfants, conn)
        if not df_pietons_enfants.empty:
            total_enfants_victimes_pietons += df_pietons_enfants.iloc[0]['enfants_victimes']
            total_enfants_victimes += df_pietons_enfants.iloc[0]['enfants_victimes']
        query_vehicules_pietons = f'''
        SELECT v.catv, COUNT(*) AS nombre
        FROM usagers u
        JOIN caract c ON u.Num_Acc = c.Num_Acc
        JOIN vehicules v ON u.Num_Acc = v.Num_Acc
        WHERE u.grav != 1 AND u.catu = 3 AND c.com = '{code_insee}'
        GROUP BY v.catv
        '''
        vehicules_pietons = pd.read_sql_query(query_vehicules_pietons, conn)
        if not vehicules_pietons.empty:
            vehicules_pietons_groupes_consolide = pd.concat([vehicules_pietons_groupes_consolide, vehicules_pietons], ignore_index=True)
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
        if not df_cyclistes.empty:
            df_cyclistes_consolide = pd.concat([df_cyclistes_consolide, df_cyclistes], ignore_index=True)
        query_cyclistes_enfants = f'''
        SELECT
            COUNT(*) AS enfants_victimes
        FROM usagers u
        JOIN caract c ON u.Num_Acc = c.Num_Acc
        JOIN vehicules v ON u.Num_Acc = v.Num_Acc AND u.num_veh = v.num_veh
        WHERE u.grav !=1 AND u.catu = 1 AND (v.catv = '01' OR v.catv = '80') AND c.com = '{code_insee}' AND u.an_nais > {ANNEE_MIN_ENFANT}
        '''
        df_cyclistes_enfants = pd.read_sql_query(query_cyclistes_enfants, conn)
        if not df_cyclistes_enfants.empty:
            total_enfants_victimes_cyclistes += df_cyclistes_enfants.iloc[0]['enfants_victimes']
            total_enfants_victimes += df_cyclistes_enfants.iloc[0]['enfants_victimes']
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
        if not vehicules_cyclistes.empty:
            vehicules_cyclistes_groupes_consolide = pd.concat([vehicules_cyclistes_groupes_consolide, vehicules_cyclistes], ignore_index=True)
        conn.close()
    # Calcul des totaux consolidés
    pietons = {
        'total_victimes': df_pietons_consolide['total_victimes'].sum() if not df_pietons_consolide.empty else 0,
        'tues': df_pietons_consolide['tues'].sum() if not df_pietons_consolide.empty else 0,
        'hospitalises': df_pietons_consolide['hospitalises'].sum() if not df_pietons_consolide.empty else 0,
        'legers': df_pietons_consolide['legers'].sum() if not df_pietons_consolide.empty else 0,
    }
    cyclistes = {
        'total_victimes': df_cyclistes_consolide['total_victimes'].sum() if not df_cyclistes_consolide.empty else 0,
        'tues': df_cyclistes_consolide['tues'].sum() if not df_cyclistes_consolide.empty else 0,
        'hospitalises': df_cyclistes_consolide['hospitalises'].sum() if not df_cyclistes_consolide.empty else 0,
        'legers': df_cyclistes_consolide['legers'].sum() if not df_cyclistes_consolide.empty else 0,
    }
    vehicules_pietons_groupes = grouper_catv(vehicules_pietons_groupes_consolide)
    vehicules_cyclistes_groupes = grouper_catv(vehicules_cyclistes_groupes_consolide)
    # Générer le rapport consolidé
    pietons_blesses_intro = pietons['total_victimes']
    cyclistes_blesses_intro = cyclistes['total_victimes']
    total_blesses_intro = pietons_blesses_intro + cyclistes_blesses_intro
    rapport = f"""
## Analyse des accidents routiers {annee} pour la commune de {nom_commune}
En {annee}, **{total_blesses_intro} personne{'s' if total_blesses_intro > 1  else ''}** à pied ou à vélo dont **{total_enfants_victimes} enfant{'s' if total_enfants_victimes > 1 else ''}** {'ont' if total_blesses_intro > 1 else 'a'} été blessée{'s' if total_blesses_intro > 1  else ''} ou tuée{'s' if total_blesses_intro > 1  else ''} dans la ville :\n\n
🔵 **{pietons_blesses_intro} piéton{'s' if pietons_blesses_intro > 1  else ''}** (dont **{total_enfants_victimes_pietons} enfant{'s' if total_enfants_victimes_pietons > 1  else ''}**)\n\n
🔵 **{cyclistes_blesses_intro} cycliste{'s' if cyclistes_blesses_intro > 1  else ''}** (dont **{total_enfants_victimes_cyclistes} enfant{'s' if total_enfants_victimes_cyclistes > 1  else ''}**)
## 🚶 Piétonnes et Piétons :
{'Parmi les' if pietons_blesses_intro > 1  else ''} **{pietons_blesses_intro}** piétonne{'s et' if total_blesses_intro > 1  else 'ou'} piéton{'s' if pietons_blesses_intro > 1  else ''} blessé·e{'s' if pietons_blesses_intro > 1  else ''} ou tué·e{'s' if pietons_blesses_intro > 1  else ''} dans {'des' if pietons_blesses_intro > 1 else 'un'} accident{'s' if pietons_blesses_intro > 1  else ''}, **{pietons['hospitalises']}** {'ont' if pietons['hospitalises'] > 1  else 'a'} été hospitalisé·e{'s' if pietons['hospitalises'] > 1  else ''}
    """
    if pietons['tues'] > 0:
        rapport += f", et **{pietons['tues']}** {'sont' if pietons['tues'] > 1  else 'est'} mort·e{'s' if pietons['tues'] > 1  else ''}.\n\n"
    else:
        rapport += ".\n\n"
    rapport += "### Véhicules impliqués :\n\n"
    if vehicules_pietons_groupes.empty:
        rapport += "* Aucune collision avec des véhicules externes enregistrée.\n\n"
    for _, row in vehicules_pietons_groupes.iterrows():
        rapport += f"🔵 {row['nombre']} accident{'s' if row['nombre'] > 1  else ''} impliquant un **{row['Mode_Transport']}**\n\n"
    rapport += f"""
## 🚴 **Cyclistes** :
{'Parmi les' if cyclistes_blesses_intro > 1  else ''} **{cyclistes_blesses_intro}** cycliste{'s' if cyclistes_blesses_intro > 1  else ''} blessé·e{'s' if cyclistes_blesses_intro > 1  else ''} ou tué·e{'s' if cyclistes_blesses_intro > 1  else ''} dans {'des' if cyclistes_blesses_intro > 1 else 'un'} accident{'s' if cyclistes_blesses_intro > 1  else ''}, **{cyclistes['hospitalises']}** {'ont' if cyclistes['hospitalises'] > 1  else 'a'} été hospitalisé·e{'s' if cyclistes['hospitalises'] > 1  else ''}
    """
    if cyclistes['tues'] > 0:
        rapport += f", et **{cyclistes['tues']}** {'sont' if cyclistes['tues'] > 1  else 'est'} mort·e{'s' if cyclistes['tues'] > 1  else ''}.\n\n"
    else:
        rapport += "\n\n"
    rapport += "### Véhicules impliqués :\n\n"
    if vehicules_cyclistes_groupes.empty:
        rapport += "- Aucune collision avec des véhicules externes enregistrée (victimes uniquement auto-accidentées ou indemnes).\n\n"
    else:
        for _, row in vehicules_cyclistes_groupes.iterrows():
            rapport += f"🔵 {row['nombre']} accident{'s' if row['nombre'] > 1  else ''} impliquant un **{row['Mode_Transport']}**\n\n"
    # Extraire et afficher les accidents par date
    df_accidents_par_date = extraire_accidents_par_date(codes_insee, annee)
    df_accidents_par_date['gravite_libelle'] = df_accidents_par_date['gravite'].map(grav_dict)
    df_synthese = df_accidents_par_date[['type_usager', 'gravite']].copy()
    df_synthese['gravite_libelle'] = df_synthese['gravite'].map(grav_dict)
    tableau_synthese = pd.crosstab(
        index=df_synthese['type_usager'],
        columns=df_synthese['gravite_libelle'],
        margins=True,
        margins_name="Total"
    )
    # ordre_gravite = ['Indemne', 'Blessé léger', 'Blessé hospitalisé', 'Tué', 'Total']
    # tableau_synthese = tableau_synthese[ordre_gravite]
    # rapport += "\n\n"
    # rapport += "### 📊 Tableau synthétique des accidents par gravité et mode de déplacement :\n\n"
    # rapport += tableau_synthese.to_markdown()
    if not df_accidents_par_date.empty:
        tableau = df_accidents_par_date[['Num_Acc', 'date_accident', 'type_usager', 'gravite_libelle', 'vehicules_impliques', 'adresse', 'latitude', 'longitude']]
        tableau = tableau.rename(columns={
            'Num_Acc': 'ID Accident',
            'date_accident': 'Date',
            'type_usager': 'Type usager',
            'gravite_libelle': 'Gravité',
            'vehicules_impliques': 'Véhicules impliqués',
            'adresse' : 'Localisation',
            'latitude': 'Latitude',
            'longitude': 'Longitude'
        })
        # Sauvegarder une copie pour le CSV
        tableau_to_csv = tableau.copy()
        tableau.drop(axis=1, columns=['Latitude', 'Longitude'], inplace=True)
        rapport_tableau = "\n\n" + tableau.to_markdown(index=False)
    else:
        rapport_tableau = "- Aucune victime recensée.\n"
    return rapport, rapport_tableau, tableau_to_csv if not df_accidents_par_date.empty else None

# Interface Streamlit
st.set_page_config(page_title="Routes mortelles")
st.title("Analyse des accidents routiers par commune")
st.markdown("Un outil créé par [LtdlGuidon](https://piaille.fr/@LTDLGuidon), pour analyser les données d'accidentologie, avec un focus sur les personnes à pied ou à vélo. Les données sont disponibles en opendata [sur datagouv](https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024/). "
            "Le code est visible [sur Github](https://github.com/LaTeteDansLeGuidon/routes_mortelles). Il s'agit d'un travail amateur, des erreurs s'y glissent peut-être... N'hésitez pas à les signaler !\n\n"
            "Pour accéder aux données d'une commune, sélectionner la commune et l'année, puis appuyer sur le bouton **Analyser**.")

# Sélection de la commune
selected_commune = st.selectbox(
    options=communes_df['nom_et_insee'],
    index=None,
    label="Sélectionnez une commune (*recherche sensible à la casse, mettre une majuscule au début du nom*)",
    label_visibility='visible'
)

# Récupérer le code INSEE de la commune sélectionnée
if selected_commune:
    selected_commune_code = selected_commune.split("INSEE : ")[1]
    # Vérifier si la commune sélectionnée est une commune parent
    enfants = communes_df[communes_df['COMPARENT'] == selected_commune_code]
    nom_commune = selected_commune.split(" - ")[0]
    if not enfants.empty:
        codes_insee = [selected_commune_code] + enfants['COM'].tolist()
    else:
        codes_insee = [selected_commune_code]

# Menu déroulant pour sélectionner l'année
annees_disponibles = list(range(2023, 2025))
index_par_defaut = annees_disponibles.index(annees_disponibles[-1])
annee = st.selectbox("Sélectionnez l'année", annees_disponibles, index=index_par_defaut)

# Bouton pour lancer l'analyse
if st.button("Analyser"):
    st.session_state.show_tableau = False
    if 'codes_insee' in locals() and len(codes_insee) > 0:
        with st.spinner("Analyse en cours..."):
            st.session_state.rapport_part1, st.session_state.rapport_tableau, st.session_state.tableau_to_csv = analyser_accidents_commune(nom_commune, codes_insee, annee)
    else:
        st.error("Aucun code INSEE valide sélectionné.")


# Affichage du rapport et du tableau détaillé (en dehors du bloc du bouton "Analyser")
if st.session_state.rapport_part1 is not None:
    st.markdown(st.session_state.rapport_part1)

    # Bouton pour afficher/masquer le tableau détaillé
    if st.button("Afficher la liste détaillée"):
        st.session_state.show_tableau = not st.session_state.get("show_tableau", False)

    # Afficher le tableau détaillé si l'état est True
    if st.session_state.get("show_tableau", False):
        st.markdown("### 📋 Liste des victimes piétonnes et cyclistes recensées dans la commune (tri par date)")
        if st.session_state.tableau_to_csv is not None:
            csv = st.session_state.tableau_to_csv.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Télécharger le tableau en CSV",
                data=csv,
                file_name=f'accidents_{codes_insee[0]}_{annee}.csv',
                mime='text/csv',
            )
        st.markdown(st.session_state.rapport_tableau)

    
    
