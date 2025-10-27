"""
Requêtes SQL pour le dashboard CHU
Basées sur les besoins utilisateurs identifiés
"""

# Requêtes pour les hospitalisations
HOSPITALIZATION_QUERIES = {
    "taux_global": """
        SELECT 
            EXTRACT(YEAR FROM date_hospitalisation) as annee,
            COUNT(*) as total_hospitalisations,
            COUNT(DISTINCT patient_id) as patients_uniques,
            ROUND(COUNT(*) * 100.0 / COUNT(DISTINCT patient_id), 2) as taux_hospitalisation
        FROM lakehouse.s3_files_views.hospitalisation 
        WHERE date_hospitalisation BETWEEN ? AND ?
        GROUP BY EXTRACT(YEAR FROM date_hospitalisation)
        ORDER BY annee
    """,
    
    "par_sexe": """
        SELECT 
            sexe,
            COUNT(*) as nombre_hospitalisations,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pourcentage
        FROM lakehouse.s3_files_views.hospitalisation 
        WHERE date_hospitalisation BETWEEN ? AND ?
        GROUP BY sexe
        ORDER BY nombre_hospitalisations DESC
    """,
    
    "par_age": """
        SELECT 
            CASE 
                WHEN age < 18 THEN '0-17'
                WHEN age BETWEEN 18 AND 35 THEN '18-35'
                WHEN age BETWEEN 36 AND 50 THEN '36-50'
                WHEN age BETWEEN 51 AND 65 THEN '51-65'
                ELSE '65+'
            END as tranche_age,
            COUNT(*) as nombre_hospitalisations
        FROM lakehouse.s3_files_views.hospitalisation 
        WHERE date_hospitalisation BETWEEN ? AND ?
        GROUP BY tranche_age
        ORDER BY 
            CASE 
                WHEN age < 18 THEN 1
                WHEN age BETWEEN 18 AND 35 THEN 2
                WHEN age BETWEEN 36 AND 50 THEN 3
                WHEN age BETWEEN 51 AND 65 THEN 4
                ELSE 5
            END
    """,
    
    "par_diagnostic": """
        SELECT 
            diagnostic,
            COUNT(*) as nombre_hospitalisations,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pourcentage
        FROM lakehouse.s3_files_views.hospitalisation 
        WHERE date_hospitalisation BETWEEN ? AND ?
        GROUP BY diagnostic
        ORDER BY nombre_hospitalisations DESC
        LIMIT 10
    """
}

# Requêtes pour les consultations
CONSULTATION_QUERIES = {
    "par_etablissement": """
        SELECT 
            e.nom_etablissement,
            COUNT(c.consultation_id) as nombre_consultations,
            COUNT(DISTINCT c.patient_id) as patients_uniques,
            ROUND(COUNT(c.consultation_id) * 100.0 / COUNT(DISTINCT c.patient_id), 2) as taux_consultation
        FROM lakehouse.s3_files_views.consultation c
        JOIN lakehouse.s3_files_views.etablissement_sante e 
            ON c.etablissement_id = e.etablissement_id
        WHERE c.date_consultation BETWEEN ? AND ?
        GROUP BY e.nom_etablissement
        ORDER BY nombre_consultations DESC
    """,
    
    "par_professionnel": """
        SELECT 
            p.profession,
            p.specialite,
            COUNT(c.consultation_id) as nombre_consultations,
            ROUND(AVG(s.score_satisfaction), 2) as score_satisfaction_moyen
        FROM lakehouse.s3_files_views.consultation c
        JOIN lakehouse.s3_files_views.professionnel_sante p 
            ON c.professionnel_id = p.identifiant
        LEFT JOIN lakehouse.s3_files_views.satisfaction s 
            ON c.consultation_id = s.consultation_id
        WHERE c.date_consultation BETWEEN ? AND ?
        GROUP BY p.profession, p.specialite
        ORDER BY nombre_consultations DESC
    """,
    
    "par_diagnostic": """
        SELECT 
            diagnostic,
            COUNT(*) as nombre_consultations,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pourcentage
        FROM lakehouse.s3_files_views.consultation 
        WHERE date_consultation BETWEEN ? AND ?
        GROUP BY diagnostic
        ORDER BY nombre_consultations DESC
        LIMIT 10
    """
}

# Requêtes pour les décès
DEATH_QUERIES = {
    "par_region_2019": """
        SELECT 
            region,
            COUNT(*) as nombre_deces
        FROM lakehouse.s3_files_views.deces 
        WHERE EXTRACT(YEAR FROM date_deces) = 2019
        GROUP BY region
        ORDER BY nombre_deces DESC
    """,
    
    "par_age_sexe": """
        SELECT 
            sexe,
            CASE 
                WHEN age < 18 THEN '0-17'
                WHEN age BETWEEN 18 AND 35 THEN '18-35'
                WHEN age BETWEEN 36 AND 50 THEN '36-50'
                WHEN age BETWEEN 51 AND 65 THEN '51-65'
                ELSE '65+'
            END as tranche_age,
            COUNT(*) as nombre_deces
        FROM lakehouse.s3_files_views.deces 
        WHERE date_deces BETWEEN ? AND ?
        GROUP BY sexe, tranche_age
        ORDER BY sexe, tranche_age
    """,
    
    "evolution_temporelle": """
        SELECT 
            EXTRACT(YEAR FROM date_deces) as annee,
            EXTRACT(MONTH FROM date_deces) as mois,
            COUNT(*) as nombre_deces
        FROM lakehouse.s3_files_views.deces 
        WHERE date_deces BETWEEN ? AND ?
        GROUP BY EXTRACT(YEAR FROM date_deces), EXTRACT(MONTH FROM date_deces)
        ORDER BY annee, mois
    """
}

# Requêtes pour la satisfaction
SATISFACTION_QUERIES = {
    "par_region_2020": """
        SELECT 
            region,
            ROUND(AVG(score_all_rea_ajust), 2) as score_moyen,
            SUM(nb_rep_score_all_rea_ajust) as nombre_reponses
        FROM lakehouse.s3_files_views.recueil 
        WHERE EXTRACT(YEAR FROM date_evaluation) = 2020
        GROUP BY region
        ORDER BY score_moyen DESC
    """,
    
    "par_critere": """
        SELECT 
            'Accueil' as critere,
            ROUND(AVG(score_accueil_rea_ajust), 2) as score_moyen
        FROM lakehouse.s3_files_views.recueil 
        WHERE date_evaluation BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            'Soins infirmiers' as critere,
            ROUND(AVG(score_PECinf_rea_ajust), 2) as score_moyen
        FROM lakehouse.s3_files_views.recueil 
        WHERE date_evaluation BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            'Soins médicaux' as critere,
            ROUND(AVG(score_PECmed_rea_ajust), 2) as score_moyen
        FROM lakehouse.s3_files_views.recueil 
        WHERE date_evaluation BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            'Chambre' as critere,
            ROUND(AVG(score_chambre_rea_ajust), 2) as score_moyen
        FROM lakehouse.s3_files_views.recueil 
        WHERE date_evaluation BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            'Repas' as critere,
            ROUND(AVG(score_repas_rea_ajust), 2) as score_moyen
        FROM lakehouse.s3_files_views.recueil 
        WHERE date_evaluation BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            'Sortie' as critere,
            ROUND(AVG(score_sortie_rea_ajust), 2) as score_moyen
        FROM lakehouse.s3_files_views.recueil 
        WHERE date_evaluation BETWEEN ? AND ?
        
        ORDER BY score_moyen DESC
    """,
    
    "evolution_satisfaction": """
        SELECT 
            EXTRACT(YEAR FROM date_evaluation) as annee,
            ROUND(AVG(score_all_rea_ajust), 2) as score_moyen_global
        FROM lakehouse.s3_files_views.recueil 
        WHERE date_evaluation BETWEEN ? AND ?
        GROUP BY EXTRACT(YEAR FROM date_evaluation)
        ORDER BY annee
    """
}

# Requêtes pour les établissements
ESTABLISHMENT_QUERIES = {
    "performance": """
        SELECT 
            e.nom_etablissement,
            e.region,
            COUNT(DISTINCT h.hospitalisation_id) as total_hospitalisations,
            ROUND(AVG(s.score_all_rea_ajust), 2) as score_satisfaction_moyen,
            COUNT(DISTINCT p.identifiant) as nombre_professionnels
        FROM lakehouse.s3_files_views.etablissement_sante e
        LEFT JOIN lakehouse.s3_files_views.hospitalisation h 
            ON e.etablissement_id = h.etablissement_id
        LEFT JOIN lakehouse.s3_files_views.recueil s 
            ON e.finess = s.finess
        LEFT JOIN lakehouse.s3_files_views.professionnel_sante p 
            ON e.etablissement_id = p.etablissement_id
        WHERE h.date_hospitalisation BETWEEN ? AND ?
        GROUP BY e.nom_etablissement, e.region
        ORDER BY total_hospitalisations DESC
    """,
    
    "par_type": """
        SELECT 
            type_etablissement,
            COUNT(*) as nombre_etablissements,
            ROUND(AVG(score_satisfaction), 2) as score_satisfaction_moyen
        FROM lakehouse.s3_files_views.etablissement_sante e
        LEFT JOIN lakehouse.s3_files_views.recueil r ON e.finess = r.finess
        GROUP BY type_etablissement
        ORDER BY nombre_etablissements DESC
    """
}

# Requêtes pour les professionnels
PROFESSIONAL_QUERIES = {
    "par_profession": """
        SELECT 
            profession,
            specialite,
            COUNT(*) as nombre_professionnels,
            COUNT(DISTINCT commune) as nombre_communes
        FROM lakehouse.s3_files_views.professionnel_sante
        GROUP BY profession, specialite
        ORDER BY nombre_professionnels DESC
    """,
    
    "consultations_par_professionnel": """
        SELECT 
            p.nom,
            p.prenom,
            p.profession,
            p.specialite,
            COUNT(c.consultation_id) as nombre_consultations,
            ROUND(AVG(s.score_satisfaction), 2) as score_satisfaction_moyen
        FROM lakehouse.s3_files_views.professionnel_sante p
        LEFT JOIN lakehouse.s3_files_views.consultation c 
            ON p.identifiant = c.professionnel_id
        LEFT JOIN lakehouse.s3_files_views.satisfaction s 
            ON c.consultation_id = s.consultation_id
        WHERE c.date_consultation BETWEEN ? AND ?
        GROUP BY p.identifiant, p.nom, p.prenom, p.profession, p.specialite
        ORDER BY nombre_consultations DESC
        LIMIT 20
    """
}

# Requêtes de métriques globales
OVERVIEW_QUERIES = {
    "metriques_globales": """
        SELECT 
            'Hospitalisations' as metrique,
            COUNT(*) as valeur
        FROM lakehouse.s3_files_views.hospitalisation 
        WHERE date_hospitalisation BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            'Décès' as metrique,
            COUNT(*) as valeur
        FROM lakehouse.s3_files_views.deces 
        WHERE date_deces BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            'Établissements' as metrique,
            COUNT(DISTINCT etablissement_id) as valeur
        FROM lakehouse.s3_files_views.etablissement_sante
        
        UNION ALL
        
        SELECT 
            'Professionnels' as metrique,
            COUNT(*) as valeur
        FROM lakehouse.s3_files_views.professionnel_sante
    """,
    
    "evolution_mensuelle": """
        SELECT 
            EXTRACT(YEAR FROM date_hospitalisation) as annee,
            EXTRACT(MONTH FROM date_hospitalisation) as mois,
            COUNT(*) as hospitalisations
        FROM lakehouse.s3_files_views.hospitalisation 
        WHERE date_hospitalisation BETWEEN ? AND ?
        GROUP BY EXTRACT(YEAR FROM date_hospitalisation), EXTRACT(MONTH FROM date_hospitalisation)
        ORDER BY annee, mois
    """
}
