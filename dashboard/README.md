# 🏥 CHU - Dashboard Big Data

## 📋 Description
Dashboard Streamlit pour l'analyse de données de santé (hospitalisations, consultations, décès, satisfaction).

## 🎯 Besoins Couverts

### ✅ Couverts (6/8)
1. ✅ Taux de consultation par diagnostic
2. ✅ Taux global d'hospitalisation
3. ✅ Taux d'hospitalisation par diagnostic
4. ✅ Taux d'hospitalisation par sexe et âge
5. ✅ Taux de consultation par professionnel
6. ✅ Nombre de décès par localisation 2019

### ❌ Impossible (1/8)
- Consultation par établissement (pas d'`id_etablissement` dans données)

### ⚠️ Partiel (1/8)
- Satisfaction 2017 globale (pas de localisation possible)

## 🚀 Installation

```bash
pip install -r requirements.txt
```

## ▶️ Lancement

```bash
streamlit run dashboard.py
# ou
python -m streamlit run dashboard.py
```

## 📊 Structure

### Sections du Dashboard
1. **Vue d'ensemble** : Métriques clés + Évolution temporelle
2. **Hospitalisations** : Globale + Par diagnostic + Par sexe/âge
3. **Consultations** : Par diagnostic + Par professionnel
4. **Décès** : Par localisation (2019)
5. **Satisfaction** : Global (2017)

### Filtres
- **Date** : Disponible pour Vue d'ensemble, Hospitalisations, Consultations
- **Région** : Disponible pour Vue d'ensemble, Hospitalisations, Consultations
- **Décès** : Forcé à 2019 (pas de filtres)
- **Satisfaction** : Global 2017 (pas de filtres)

## 🗂️ Fichiers Principaux

- `dashboard.py` : Application Streamlit principale
- `queries.py` : Fonctions de requêtes SQL
- `requirements.txt` : Dépendances Python
- `diagnostic_motherduck.py` : Diagnostic de connexion MotherDuck

## 🔧 Technologies

- **Streamlit** : Framework web
- **DuckDB** : Base de données OLAP
- **MotherDuck** : Data warehouse cloud
- **Plotly** : Visualisations interactives
- **Pandas** : Manipulation de données

## 📝 Notes Techniques

### Limitations de Données
- **Consultations** : Pas d'établissement (`id_etablissement = NULL`)
- **Satisfaction** : Pas de date (`id_temps = NULL`) ni localisation (`code_lieu = NULL`)
- **Régions** : Mappage via code postal (2 premiers chiffres)

### Configuration
- Période par défaut : 2019-2020
- Filtrage régional : Basé sur département (code postal)
- Données : `warehouse.hypercube.*` (Cube OLAP)

