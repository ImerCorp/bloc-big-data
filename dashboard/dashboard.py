import os
import duckdb
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from dotenv import dotenv_values, load_dotenv
from pathlib import Path

# Configuration de la page
st.set_page_config(
    page_title="CHU - Tableau de Bord Santé",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment variables from .env file
load_dotenv('/app/.env')
# Or simply (since /app is the working directory)
load_dotenv()

# Configuration de la connexion MotherDuck
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")
# Mapping code postal → région (basé sur les 2 premiers chiffres)
CODE_POSTAL_TO_REGION = {
    # Île-de-France
    '75': 'Île-de-France', '77': 'Île-de-France', '78': 'Île-de-France', '91': 'Île-de-France', '92': 'Île-de-France', '93': 'Île-de-France', '94': 'Île-de-France', '95': 'Île-de-France',
    # Auvergne-Rhône-Alpes
    '01': 'Auvergne-Rhône-Alpes', '03': 'Auvergne-Rhône-Alpes', '07': 'Auvergne-Rhône-Alpes', '15': 'Auvergne-Rhône-Alpes', '26': 'Auvergne-Rhône-Alpes', '38': 'Auvergne-Rhône-Alpes', '42': 'Auvergne-Rhône-Alpes', '43': 'Auvergne-Rhône-Alpes', '63': 'Auvergne-Rhône-Alpes', '69': 'Auvergne-Rhône-Alpes', '73': 'Auvergne-Rhône-Alpes', '74': 'Auvergne-Rhône-Alpes',
    # Occitanie
    '09': 'Occitanie', '11': 'Occitanie', '12': 'Occitanie', '30': 'Occitanie', '31': 'Occitanie', '32': 'Occitanie', '34': 'Occitanie', '46': 'Occitanie', '48': 'Occitanie', '65': 'Occitanie', '66': 'Occitanie', '81': 'Occitanie', '82': 'Occitanie',
    # Nouvelle-Aquitaine
    '16': 'Nouvelle-Aquitaine', '17': 'Nouvelle-Aquitaine', '19': 'Nouvelle-Aquitaine', '23': 'Nouvelle-Aquitaine', '24': 'Nouvelle-Aquitaine', '33': 'Nouvelle-Aquitaine', '40': 'Nouvelle-Aquitaine', '47': 'Nouvelle-Aquitaine', '64': 'Nouvelle-Aquitaine', '79': 'Nouvelle-Aquitaine', '86': 'Nouvelle-Aquitaine', '87': 'Nouvelle-Aquitaine',
    # Hauts-de-France
    '02': 'Hauts-de-France', '59': 'Hauts-de-France', '60': 'Hauts-de-France', '62': 'Hauts-de-France', '80': 'Hauts-de-France',
    # Provence-Alpes-Côte d'Azur
    '04': 'Provence-Alpes-Côte d\'Azur', '05': 'Provence-Alpes-Côte d\'Azur', '06': 'Provence-Alpes-Côte d\'Azur', '13': 'Provence-Alpes-Côte d\'Azur', '83': 'Provence-Alpes-Côte d\'Azur', '84': 'Provence-Alpes-Côte d\'Azur',
    # Grand Est
    '08': 'Grand Est', '10': 'Grand Est', '51': 'Grand Est', '52': 'Grand Est', '54': 'Grand Est', '55': 'Grand Est', '57': 'Grand Est', '67': 'Grand Est', '68': 'Grand Est', '88': 'Grand Est',
    # Normandie
    '14': 'Normandie', '27': 'Normandie', '50': 'Normandie', '61': 'Normandie', '76': 'Normandie',
    # Pays de la Loire
    '44': 'Pays de la Loire', '49': 'Pays de la Loire', '53': 'Pays de la Loire', '72': 'Pays de la Loire', '85': 'Pays de la Loire',
    # Bretagne
    '22': 'Bretagne', '29': 'Bretagne', '35': 'Bretagne', '56': 'Bretagne',
    # Centre-Val de Loire
    '18': 'Centre-Val de Loire', '28': 'Centre-Val de Loire', '36': 'Centre-Val de Loire', '37': 'Centre-Val de Loire', '41': 'Centre-Val de Loire', '45': 'Centre-Val de Loire',
    # Bourgogne-Franche-Comté
    '21': 'Bourgogne-Franche-Comté', '25': 'Bourgogne-Franche-Comté', '39': 'Bourgogne-Franche-Comté', '58': 'Bourgogne-Franche-Comté', '70': 'Bourgogne-Franche-Comté', '71': 'Bourgogne-Franche-Comté', '89': 'Bourgogne-Franche-Comté', '90': 'Bourgogne-Franche-Comté',
    # Corse
    '20': 'Corse',  # Note: 20 est aussi Ajaccio, mais on considère 20 comme Corse principalement
}

def get_region_from_code_postal(code):
    """Extrait la région depuis un code postal"""
    if code is None or code == "":
        return None
    # Prendre les 2 premiers chiffres du code
    dept = str(code)[:2]
    return CODE_POSTAL_TO_REGION.get(dept, None)

def get_connection():
    """Connexion à MotherDuck"""
    try:
        conn = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")
        # Attacher le lakehouse si nécessaire
        try:
            conn.execute("ATTACH 'md:lakehouse'")
        except:
            pass  # Déjà attaché
        return conn
    except Exception as e:
        st.error(f"Erreur de connexion: {e}")
        return None

@st.cache_data
def execute_query(query, params=None):
    """Exécuter une requête avec cache"""
    conn = get_connection()
    if conn is None:
        return None
    
    try:
        if params:
            result = conn.execute(query, params)
        else:
            result = conn.execute(query)
        
        data = result.fetchall()
        columns = [desc[0] for desc in result.description]
        df = pd.DataFrame(data, columns=columns)
        
        # Fermer la connexion
        conn.close()
        
        return df
    except Exception as e:
        st.error(f"Erreur requête: {e}")
        if conn:
            conn.close()
        return None

def main():
    # En-tête principal
    st.title("🏥 CHU - Tableau de Bord Santé")
    st.markdown("**Cloud Healthcare Unit - Système Décisionnel Big Data**")
    
    # Sidebar pour la navigation
    st.sidebar.title("📊 Navigation")
    analysis_type = st.sidebar.selectbox(
        "Type d'analyse",
        [
            "📈 Vue d'ensemble",
            "🏥 Hospitalisations", 
            "👥 Consultations",
            "🕊️ Décès",
            "😊 Satisfaction Globale"
        ]
    )
    
    # Filtres communs
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 Filtres")
    
    # Déterminer quels filtres afficher selon l'analyse
    if analysis_type == "🕊️ Décès":
        # Décès forcé à 2019, pas de filtre de date ni région
        st.sidebar.info("📅 **Décès** : Analyse limitée à l'année 2019\n❌ Pas de filtres date/région")
        start_date = datetime(2019, 1, 1).date()
        end_date = datetime(2019, 12, 31).date()
        region = "Toutes"
    elif analysis_type == "😊 Satisfaction Globale":
        # Satisfaction pas de localisation possible
        st.sidebar.info("📅 **Satisfaction** : Données 2017 globales\n❌ Pas de filtres date/région")
        start_date = datetime(2017, 1, 1).date()
        end_date = datetime(2017, 12, 31).date()
        region = "Toutes"
    else:
        # Pour les autres analyses, afficher les filtres
        # Période
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Date début", value=datetime(2019, 1, 1))
        with col2:
            end_date = st.date_input("Date fin", value=datetime(2020, 12, 31))
        
        # Région - Filtre basé sur code_lieu (code postal)
        region = st.sidebar.selectbox(
            "Région",
            [
                "Toutes",
                "Auvergne-Rhône-Alpes",
                "Bourgogne-Franche-Comté",
                "Bretagne",
                "Centre-Val de Loire",
                "Corse",
                "Grand Est",
                "Hauts-de-France",
                "Île-de-France",
                "Normandie",
                "Nouvelle-Aquitaine",
                "Occitanie",
                "Pays de la Loire",
                "Provence-Alpes-Côte d'Azur"
            ]
        )
    
    # Contenu principal selon l'analyse sélectionnée
    if analysis_type == "📈 Vue d'ensemble":
        show_overview(start_date, end_date, region)
    elif analysis_type == "🏥 Hospitalisations":
        show_hospitalizations(start_date, end_date, region)
    elif analysis_type == "👥 Consultations":
        show_consultations(start_date, end_date, region)
    elif analysis_type == "🕊️ Décès":
        show_deaths(start_date, end_date, region)
    elif analysis_type == "😊 Satisfaction Globale":
        show_satisfaction(start_date, end_date, region)

def show_overview(start_date, end_date, region):
    """Vue d'ensemble du système"""
    st.header("📈 Vue d'ensemble du système")
    
    # Métriques principales avec vraies données
    col1, col2, col3, col4 = st.columns(4)
    
    # Récupérer les vraies données depuis l'entrepôt OLAP
    try:
        # Total hospitalisations depuis la table de faits
        # Filtre par région via code_lieu (code postal)
        if region == "Toutes":
            region_filter = ""
            region_param = []
        else:
            # Trouver les départements de la région sélectionnée
            depts = [dept for dept, reg in CODE_POSTAL_TO_REGION.items() if reg == region]
            if depts:
                # Créer une condition SQL pour vérifier les 2 premiers chiffres du code_lieu
                conditions = " OR ".join([f"SUBSTRING(CAST(l.code_lieu AS VARCHAR), 1, 2) = '{dept}'" for dept in depts])
                region_filter = f"AND ({conditions})"
            else:
                region_filter = ""
            region_param = []
        
        df_hosp = execute_query(f"""
            SELECT SUM(f.nombre_hospitalisation) as total 
            FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
            JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
            {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
            WHERE f.type_evenement = 'HOSPITALISATION'
            AND t.date_complete >= ? AND t.date_complete <= ?
            {region_filter}
        """, [start_date, end_date] + region_param)
        total_hosp = df_hosp['total'].iloc[0] if df_hosp is not None and not df_hosp.empty and df_hosp['total'].iloc[0] is not None else 0
        
        # Total décès depuis la table de faits
        df_deces = execute_query(f"""
            SELECT SUM(f.nombre_deces) as total 
            FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
            JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
            {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
            WHERE f.type_evenement = 'DECES'
            AND t.date_complete >= ? AND t.date_complete <= ?
            {region_filter}
        """, [start_date, end_date] + region_param)
        total_deces = df_deces['total'].iloc[0] if df_deces is not None and not df_deces.empty and df_deces['total'].iloc[0] is not None else 0
        
        # Taux de satisfaction moyen depuis la table de faits
        # Note: Les données de satisfaction n'ont pas de date (id_temps = NULL)
        # et pas de code_lieu, donc pas de filtre par région
        # Données de 2017, donc on affiche toujours le score moyen global
        df_satisfaction = execute_query("""
            SELECT AVG(f.score_satisfaction) as score_moyen 
            FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
            WHERE f.type_evenement = 'SATISFACTION'
            AND f.score_satisfaction IS NOT NULL
        """, [])
        score_satisfaction = df_satisfaction['score_moyen'].iloc[0] if df_satisfaction is not None and not df_satisfaction.empty and df_satisfaction['score_moyen'].iloc[0] is not None else None
        
        # Nombre d'établissements depuis la dimension
        df_etab = execute_query("SELECT COUNT(*) as total FROM warehouse.hypercube.DIM_ETABLISSEMENT")
        total_etab = df_etab['total'].iloc[0] if df_etab is not None and not df_etab.empty and df_etab['total'].iloc[0] is not None else 0
        
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {e}")
        total_hosp = total_deces = total_etab = 0
        score_satisfaction = None
    
    with col1:
        st.metric(
            label="Total Hospitalisations (filtré par date)",
            value=f"{total_hosp:,}" if total_hosp is not None else "0",
            delta="Données réelles"
        )
    
    with col2:
        st.metric(
            label="Total Décès (filtré par date)",
            value=f"{total_deces:,}" if total_deces is not None else "0",
            delta="Données réelles"
        )
    
    with col3:
        # Afficher satisfaction si disponible
        if score_satisfaction is not None and score_satisfaction > 0:
            st.metric(
                label="Taux de Satisfaction Globale (non filtré)",
                value=f"{score_satisfaction:.1f}/100",
                delta="Données 2017"
            )
        else:
            st.metric(
                label="Taux de Satisfaction Globale (non filtré)",
                value="N/A",
                delta="Disponible"
            )
    
    with col4:
        st.metric(
            label="Établissements Actifs (filtré par date)",
            value=f"{total_etab:,}" if total_etab is not None else "0",
            delta="Données réelles"
        )
    
    # Graphique d'évolution temporelle
    st.markdown("---")
    st.subheader("📊 Évolution des Hospitalisations (filtré par date)")
    
    try:
        # Récupérer les données filtrées par date
        query_evo = f"""
            SELECT 
                t.date_complete as Date,
                SUM(f.nombre_hospitalisation) as hospitalisations
            FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
            JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
            {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
            WHERE f.type_evenement = 'HOSPITALISATION'
            AND t.date_complete >= ? AND t.date_complete <= ?
            {region_filter}
            GROUP BY t.date_complete
            ORDER BY t.date_complete
        """
        
        df_evo = execute_query(query_evo, [start_date, end_date] + region_param)
        
        if df_evo is not None and not df_evo.empty:
            fig = px.line(df_evo, x='Date', y='hospitalisations', 
                         title='Evolution temporelle (filtré par date et région)',
                         labels={'hospitalisations': 'Nombre d\'hospitalisations', 'Date': 'Date'},
                         color_discrete_sequence=['#2196f3'])
            fig.update_traces(mode='lines+markers', line=dict(width=2))
            fig.update_layout(
                xaxis_title='Date',
                yaxis_title='Nombre d\'hospitalisations',
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune donnée d'évolution disponible")
    except Exception as e:
        st.warning(f"Erreur lors du chargement: {e}")

def show_hospitalizations(start_date, end_date, region):
    """Analyse des hospitalisations"""
    st.header("🏥 Hospitalisations")
    st.markdown("**Besoins : Taux global d'hospitalisation + par diagnostic + par sexe et âge**")
    
    # Filtre par région
    if region == "Toutes":
        region_filter = ""
        region_param = []
    else:
        depts = [dept for dept, reg in CODE_POSTAL_TO_REGION.items() if reg == region]
        if depts:
            conditions = " OR ".join([f"SUBSTRING(CAST(l.code_lieu AS VARCHAR), 1, 2) = '{dept}'" for dept in depts])
            region_filter = f"AND ({conditions})"
        else:
            region_filter = ""
        region_param = []
    
    # Taux global d'hospitalisation
    try:
        query_hosp_global = f"""
                SELECT 
                    SUM(f.nombre_hospitalisation) as total_hospitalisations,
                    AVG(f.duree_hospitalisation_jours) as duree_moyenne,
                    COUNT(DISTINCT f.id_patient) as patients_uniques
                FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
                JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
                {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
                WHERE f.type_evenement = 'HOSPITALISATION'
                AND t.date_complete >= ? AND t.date_complete <= ?
                {region_filter}
        """
        df_hosp_global = execute_query(query_hosp_global, [start_date, end_date] + region_param)
        
        if df_hosp_global is not None and not df_hosp_global.empty:
            total = df_hosp_global['total_hospitalisations'].iloc[0] if df_hosp_global['total_hospitalisations'].iloc[0] is not None else 0
            duree_moy = df_hosp_global['duree_moyenne'].iloc[0] if df_hosp_global['duree_moyenne'].iloc[0] is not None else 0
            patients = df_hosp_global['patients_uniques'].iloc[0] if df_hosp_global['patients_uniques'].iloc[0] is not None else 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Hospitalisations", f"{total:,}")
            with col2:
                st.metric("Durée moyenne", f"{duree_moy:.1f} jours")
            with col3:
                st.metric("Patients uniques", f"{patients:,}")
        else:
            st.warning("Aucune donnée d'hospitalisation disponible")
            return
            
    except Exception as e:
        st.error(f"Erreur: {e}")
        return
    
    # Graphiques d'analyse avec vraies données
    st.subheader("📊 Analyses démographiques")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 👥 Hospitalisations par sexe")
        # Vraies données depuis la base
        try:
            query_sexe = f"""
                SELECT 
                    p.sexe,
                    SUM(f.nombre_hospitalisation) as total_hospitalisations
                FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
                JOIN warehouse.hypercube.DIM_PATIENT p ON f.id_patient = p.id_patient
                JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
                {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
                WHERE f.type_evenement = 'HOSPITALISATION'
                AND t.date_complete >= ? AND t.date_complete <= ?
                {region_filter}
                GROUP BY p.sexe
                ORDER BY total_hospitalisations DESC
            """
            df_sexe = execute_query(query_sexe, [start_date, end_date] + region_param)
            
            if df_sexe is not None and not df_sexe.empty:
                # Graphique pie chart avec donut élégant
                fig_pie = px.pie(df_sexe, values='total_hospitalisations', names='sexe', 
                                title='Proportion par sexe',
                                color_discrete_sequence=['#3498db', '#e74c3c'],
                                hole=0.4)
                fig_pie.update_traces(textposition='inside', textinfo='percent+label+value')
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("Aucune donnée disponible")
        except Exception as e:
            st.warning(f"Erreur: {e}")
    
    with col2:
        st.markdown("### 📅 Hospitalisations par âge")
        # Vraies données depuis la base
        try:
            query_age = f"""
                SELECT 
                    CASE 
                        WHEN p.age < 18 THEN '0-17'
                        WHEN p.age BETWEEN 18 AND 35 THEN '18-35'
                        WHEN p.age BETWEEN 36 AND 50 THEN '36-50'
                        WHEN p.age BETWEEN 51 AND 65 THEN '51-65'
                        ELSE '65+'
                    END as tranche_age,
                    SUM(f.nombre_hospitalisation) as total_hospitalisations
                FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
                JOIN warehouse.hypercube.DIM_PATIENT p ON f.id_patient = p.id_patient
                JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
                {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
                WHERE f.type_evenement = 'HOSPITALISATION'
                AND t.date_complete >= ? AND t.date_complete <= ?
                {region_filter}
                GROUP BY tranche_age
                ORDER BY 
                    CASE tranche_age
                        WHEN '0-17' THEN 1
                        WHEN '18-35' THEN 2
                        WHEN '36-50' THEN 3
                        WHEN '51-65' THEN 4
                        ELSE 5
                    END
            """
            df_age = execute_query(query_age, [start_date, end_date] + region_param)
            
            if df_age is not None and not df_age.empty:
                # Graphique par âge avec dégradé de couleur
                fig = px.bar(df_age, x='tranche_age', y='total_hospitalisations', 
                           title='Répartition par âge',
                           color='total_hospitalisations',
                           color_continuous_scale='Reds',
                           text='total_hospitalisations',
                           labels={'tranche_age': 'Tranche d\'âge', 'total_hospitalisations': 'Hospitalisations'})
                fig.update_traces(texttemplate='%{text}', textposition='outside')
                fig.update_layout(xaxis_title='Tranche d\'âge', yaxis_title='Nombre d\'hospitalisations')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Aucune donnée disponible")
        except Exception as e:
            st.warning(f"Erreur: {e}")
    
    # Hospitalisations par catégorie de diagnostic
    st.subheader("🩺 Hospitalisations par catégorie de diagnostic")
    try:
        query_diag = f"""
            SELECT 
                d.categorie_diagnostic,
                SUM(f.nombre_hospitalisation) as total_hospitalisations,
                COUNT(DISTINCT f.id_patient) as patients_uniques
            FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
            JOIN warehouse.hypercube.DIM_DIAGNOSTIC d ON f.code_diag = d.code_diag
            JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
            {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
            WHERE f.type_evenement = 'HOSPITALISATION'
            AND t.date_complete >= ? AND t.date_complete <= ?
            {region_filter}
            GROUP BY d.categorie_diagnostic
            HAVING SUM(f.nombre_hospitalisation) > 0
            ORDER BY total_hospitalisations DESC
            LIMIT 20
        """
        df_diag = execute_query(query_diag, [start_date, end_date] + region_param)
        
        if df_diag is not None and not df_diag.empty:
            fig = px.bar(df_diag, x='total_hospitalisations', y='categorie_diagnostic',
                       orientation='h',
                       title='Top catégories de diagnostics pour hospitalisations',
                       color='total_hospitalisations',
                       color_continuous_scale='Blues',
                       labels={'total_hospitalisations': 'Nombre d\'hospitalisations', 'categorie_diagnostic': 'Catégorie de diagnostic'})
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
            
            # Afficher aussi un tableau
            st.dataframe(df_diag[['categorie_diagnostic', 'total_hospitalisations', 'patients_uniques']], 
                        use_container_width=True, hide_index=True)
        else:
            st.info("Aucune donnée disponible")
    except Exception as e:
        st.warning(f"Erreur: {e}")

def show_consultations(start_date, end_date, region):
    """Analyse des consultations"""
    st.header("👥 Consultations")
    
    # Introduction / Storytelling
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **Analyse des consultations médicales** pour la période sélectionnée.
        
        Cette section présente les consultations selon deux dimensions clés :
        - 📊 **Par diagnostic** : Les motifs de consultation les plus fréquents
        - 👨‍⚕️ **Par professionnel** : Les types de professionnels les plus consultés
        """)
    
    # Filtre par région
    if region == "Toutes":
        region_filter = ""
        region_param = []
    else:
        depts = [dept for dept, reg in CODE_POSTAL_TO_REGION.items() if reg == region]
        if depts:
            conditions = " OR ".join([f"SUBSTRING(CAST(l.code_lieu AS VARCHAR), 1, 2) = '{dept}'" for dept in depts])
            region_filter = f"AND ({conditions})"
        else:
            region_filter = ""
        region_param = []
    
    try:
        # Consultations par catégorie de diagnostic - Top 15 uniquement
        st.subheader("📊 Consultations par catégorie de diagnostic")
        st.markdown("*Top 15 des catégories de diagnostics les plus fréquentes*")
        
        query_consult_diag = f"""
                SELECT 
                    d.categorie_diagnostic,
                    SUM(f.nombre_consultation) as total_consultations,
                    COUNT(DISTINCT f.id_patient) as patients_uniques
                FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
                JOIN warehouse.hypercube.DIM_DIAGNOSTIC d ON f.code_diag = d.code_diag
                JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
                {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
                WHERE f.type_evenement = 'CONSULTATION'
                AND t.date_complete >= ? AND t.date_complete <= ?
                {region_filter}
                GROUP BY d.categorie_diagnostic
                ORDER BY total_consultations DESC
                LIMIT 15
        """
        df_diag = execute_query(query_consult_diag, [start_date, end_date] + region_param)
        
        if df_diag is not None and not df_diag.empty:
            # Graphique horizontal pour meilleure lisibilité
            fig = px.bar(df_diag, x='total_consultations', y='categorie_diagnostic',
                        orientation='h',
                        title='Top 15 des catégories de diagnostics',
                        color='total_consultations',
                        color_continuous_scale='Blues',
                        labels={'total_consultations': 'Nombre de consultations', 'categorie_diagnostic': 'Catégorie de diagnostic'})
            fig.update_layout(yaxis={'categoryorder':'total ascending'}, 
                            height=600)
            # Tronquer les labels longs
            fig.update_yaxes(tickmode='linear')
            fig.update_layout(yaxis_automargin=True)
            st.plotly_chart(fig, use_container_width=True)
            
            # Tableau limité aux 10 premiers
            st.markdown("**Détail (Top 10)**")
            st.dataframe(df_diag.head(10), use_container_width=True, hide_index=True)
        else:
            st.info("Aucune donnée de consultation par diagnostic disponible")
            
    except Exception as e:
        st.warning(f"Erreur: {e}")
    
    # Séparateur
    st.markdown("---")
    
    try:
        # Consultations par professionnel - Top 15 uniquement
        st.subheader("👨‍⚕️ Consultations par professionnel")
        st.markdown("*Top 15 des professions les plus consultées*")
        
        query_consult_prof = f"""
                SELECT 
                    p.profession,
                    SUM(f.nombre_consultation) as total_consultations,
                    COUNT(DISTINCT f.id_patient) as patients_uniques
                FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
                JOIN warehouse.hypercube.DIM_PROFESSIONNEL p ON f.id_professionnel = p.identifiant
                JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
                {f"LEFT JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu" if region_filter else ""}
                WHERE f.type_evenement = 'CONSULTATION'
                AND t.date_complete >= ? AND t.date_complete <= ?
                {region_filter}
                GROUP BY p.profession
                ORDER BY total_consultations DESC
                LIMIT 15
        """
        df_prof = execute_query(query_consult_prof, [start_date, end_date] + region_param)
        
        if df_prof is not None and not df_prof.empty:
            # Graphique horizontal pour meilleure lisibilité
            fig = px.bar(df_prof, x='total_consultations', y='profession',
                        orientation='h',
                        title='Top 15 des professions',
                        color='total_consultations',
                        color_continuous_scale='Greens',
                        labels={'total_consultations': 'Nombre de consultations', 'profession': 'Profession'})
            fig.update_layout(yaxis={'categoryorder':'total ascending'},
                            height=500)
            fig.update_layout(yaxis_automargin=True)
            st.plotly_chart(fig, use_container_width=True)
            
            # Tableau limité aux 10 premiers
            st.markdown("**Détail (Top 10)**")
            st.dataframe(df_prof.head(10), use_container_width=True, hide_index=True)
        else:
            st.info("Aucune donnée de consultation par professionnel disponible")
            
    except Exception as e:
        st.warning(f"Erreur: {e}")

def show_deaths(start_date, end_date, region):
    """Analyse des décès - Besoin : Nombre de décès par localisation (région) sur l'année 2019"""
    st.header("🕊️ Décès")
    st.markdown("**Nombre de décès par localisation (région) sur l'année 2019**")
    
    # Forcer l'année 2019 comme demandé dans les besoins
    start_date = datetime(2019, 1, 1).date()
    end_date = datetime(2019, 12, 31).date()
    
    st.info(f"📅 Période analysée : {start_date} à {end_date}")
    
    st.subheader("📊 Nombre de décès par localisation")
    
    # Récupérer les vraies données depuis l'entrepôt OLAP
    try:
        # Requête pour les décès par commune depuis la table de faits
        query_deces = """
            SELECT 
                l.commune,
                SUM(f.nombre_deces) as nombre_deces
            FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
            JOIN warehouse.hypercube.DIM_LOCALISATION l ON f.code_lieu = l.code_lieu
            JOIN warehouse.hypercube.DIM_TEMPS t ON f.id_temps = t.id_temps
            WHERE f.type_evenement = 'DECES' 
            AND t.date_complete >= ? AND t.date_complete <= ?
            GROUP BY l.commune
            ORDER BY nombre_deces DESC
            LIMIT 20
        """
        df_deces = execute_query(query_deces, [start_date, end_date])
        
        if df_deces is None or df_deces.empty:
            st.warning("Aucune donnée de décès disponible pour 2019")
            return
            
    except Exception as e:
        st.error(f"Erreur lors du chargement des données de décès: {e}")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(df_deces, x='commune', y='nombre_deces', 
                    title='Décès par commune (2019)')
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.pie(df_deces, values='nombre_deces', names='commune',
                    title='Répartition des décès par commune')
        st.plotly_chart(fig, use_container_width=True)
    
    # Tableau détaillé
    st.subheader("📋 Détail par commune")
    st.dataframe(df_deces, use_container_width=True)

def show_satisfaction(start_date, end_date, region):
    """Analyse de la satisfaction globale - Données de 2017 (sans date dans la base)"""
    st.header("😊 Satisfaction Globale")
    st.markdown("**Taux global de satisfaction**")
    
    st.info("📅 Données de 2017 (période non disponible dans les données sources)")
    
    # Récupérer les vraies données de satisfaction depuis l'entrepôt OLAP
    try:
        query_satisfaction = """
            SELECT 
                COUNT(*) as nombre_reponses,
                ROUND(AVG(f.score_satisfaction), 2) as score_moyen,
                ROUND(MIN(f.score_satisfaction), 2) as score_min,
                ROUND(MAX(f.score_satisfaction), 2) as score_max,
                ROUND(STDDEV(f.score_satisfaction), 2) as score_ecart_type
            FROM warehouse.hypercube.FAIT_EVENEMENT_SANTE f
            WHERE f.type_evenement = 'SATISFACTION' 
            AND f.score_satisfaction IS NOT NULL
        """
        df_satisfaction = execute_query(query_satisfaction, [])
        
        if df_satisfaction is None or df_satisfaction.empty:
            st.warning("Aucune donnée de satisfaction disponible")
            return
        
        # Afficher les métriques globales
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Nombre de réponses", f"{df_satisfaction['nombre_reponses'].iloc[0]:,}")
        with col2:
            st.metric("Score moyen", f"{df_satisfaction['score_moyen'].iloc[0]}/100")
        with col3:
            st.metric("Score min", f"{df_satisfaction['score_min'].iloc[0]}")
        with col4:
            st.metric("Score max", f"{df_satisfaction['score_max'].iloc[0]}")
        
        st.info("ℹ️ Les données de satisfaction ne contiennent pas d'information de localisation ni de date dans la base de données.")
        
    except Exception as e:
        st.error(f"Erreur lors du chargement des données de satisfaction: {e}")

if __name__ == "__main__":
    main()