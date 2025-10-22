CREATE TABLE IF NOT EXISTS hypercube.FAIT_EVENEMENT_SANTE (
    id_evenement BIGINT PRIMARY KEY,
    type_evenement VARCHAR(50) NOT NULL, -- 'CONSULTATION', 'HOSPITALISATION', 'DECES'
    
    -- Foreign keys to dimensions
    id_temps INTEGER,
    id_patient BIGINT,
    code_diag VARCHAR(50),
    id_professionnel VARCHAR(50),
    id_etablissement VARCHAR(50),
    id_mutuelle BIGINT,
    code_lieu VARCHAR(50),
    
    -- Measures (métriques additives)
    nombre_consultation INTEGER DEFAULT 0,
    nombre_hospitalisation INTEGER DEFAULT 0,
    nombre_deces INTEGER DEFAULT 0,
    duree_hospitalisation_jours INTEGER DEFAULT 0,
    score_satisfaction DECIMAL(5,2),
    nombre_prescriptions INTEGER DEFAULT 0,
    duree_consultation_minutes INTEGER DEFAULT 0,
    
    -- Foreign key constraints
    FOREIGN KEY (id_temps) REFERENCES hypercube.DIM_TEMPS(id_temps),
    FOREIGN KEY (id_patient) REFERENCES hypercube.DIM_PATIENT(id_patient),
    FOREIGN KEY (code_diag) REFERENCES hypercube.DIM_DIAGNOSTIC(code_diag),
    FOREIGN KEY (id_professionnel) REFERENCES hypercube.DIM_PROFESSIONNEL(identifiant),
    FOREIGN KEY (id_etablissement) REFERENCES hypercube.DIM_ETABLISSEMENT(identifiant_organisation),
    FOREIGN KEY (id_mutuelle) REFERENCES hypercube.DIM_MUTUELLE(id_mut),
    FOREIGN KEY (code_lieu) REFERENCES hypercube.DIM_LOCALISATION(code_lieu)
);

-- Indexes on fact table for query performance
CREATE INDEX IF NOT EXISTS idx_fait_type ON hypercube.FAIT_EVENEMENT_SANTE(type_evenement);
CREATE INDEX IF NOT EXISTS idx_fait_temps ON hypercube.FAIT_EVENEMENT_SANTE(id_temps);
CREATE INDEX IF NOT EXISTS idx_fait_patient ON hypercube.FAIT_EVENEMENT_SANTE(id_patient);
CREATE INDEX IF NOT EXISTS idx_fait_diag ON hypercube.FAIT_EVENEMENT_SANTE(code_diag);
CREATE INDEX IF NOT EXISTS idx_fait_prof ON hypercube.FAIT_EVENEMENT_SANTE(id_professionnel);
CREATE INDEX IF NOT EXISTS idx_fait_etab ON hypercube.DIM_ETABLISSEMENT(identifiant_organisation);
CREATE INDEX IF NOT EXISTS idx_fait_lieu ON hypercube.FAIT_EVENEMENT_SANTE(code_lieu);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fait_temps_etab ON hypercube.FAIT_EVENEMENT_SANTE(id_temps, id_etablissement);
CREATE INDEX IF NOT EXISTS idx_fait_temps_diag ON hypercube.FAIT_EVENEMENT_SANTE(id_temps, code_diag);
CREATE INDEX IF NOT EXISTS idx_fait_temps_patient ON hypercube.FAIT_EVENEMENT_SANTE(id_temps, id_patient);

-- =====================================================
-- Populate FAIT_EVENEMENT_SANTE from multiple sources
-- =====================================================

-- =====================================================
-- 1. CONSULTATIONS (from PostgreSQL consultation table)
-- =====================================================

INSERT INTO hypercube.FAIT_EVENEMENT_SANTE (
    id_evenement,
    type_evenement,
    id_temps,
    id_patient,
    code_diag,
    id_professionnel,
    id_etablissement,
    id_mutuelle,
    code_lieu,
    nombre_consultation,
    nombre_hospitalisation,
    nombre_deces,
    duree_hospitalisation_jours,
    score_satisfaction,
    nombre_prescriptions,
    duree_consultation_minutes
)
SELECT 
    c."Num_consultation" AS id_evenement,
    'CONSULTATION' AS type_evenement,
    
    -- Join to DIM_TEMPS to get id_temps
    t.id_temps,
    
    -- Patient
    c."Id_patient" AS id_patient,
    
    -- Diagnostic
    c."Code_diag" AS code_diag,
    
    -- Professionnel
    c."Id_prof_sante" AS id_professionnel,
    
    -- Etablissement - NULL car non disponible directement depuis le professionnel
    NULL AS id_etablissement,
    
    -- Mutuelle
    c."Id_mut" AS id_mutuelle,
    
    -- Lieu (use patient's postal code)
    p."Code_postal" AS code_lieu,
    
    -- Measures
    1 AS nombre_consultation,
    0 AS nombre_hospitalisation,
    0 AS nombre_deces,
    0 AS duree_hospitalisation_jours,
    NULL AS score_satisfaction,
    
    -- Count prescriptions for this consultation
    (SELECT COUNT(*) 
     FROM prescription pr 
     WHERE pr."Num_consultation" = c."Num_consultation") AS nombre_prescriptions,
    
    -- Calculate duration in minutes
    CASE 
        WHEN c."Heure_fin" IS NOT NULL AND c."Heure_debut" IS NOT NULL
        THEN (EXTRACT(HOUR FROM c."Heure_fin") * 60 + EXTRACT(MINUTE FROM c."Heure_fin")) -
             (EXTRACT(HOUR FROM c."Heure_debut") * 60 + EXTRACT(MINUTE FROM c."Heure_debut"))
        ELSE NULL
    END AS duree_consultation_minutes

FROM consultation c
LEFT JOIN hypercube.DIM_TEMPS t ON c."Date" = t.date_complete
LEFT JOIN patient p ON c."Id_patient" = p."Id_patient"
LEFT JOIN hypercube.DIM_PROFESSIONNEL aps ON c."Id_prof_sante" = aps.identifiant
WHERE c."Num_consultation" IS NOT NULL;

-- =====================================================
-- 2. HOSPITALISATIONS (from S3 hospitalisation view)
-- =====================================================

INSERT INTO hypercube.FAIT_EVENEMENT_SANTE (
    id_evenement,
    type_evenement,
    id_temps,
    id_patient,
    code_diag,
    id_professionnel,
    id_etablissement,
    id_mutuelle,
    code_lieu,
    nombre_consultation,
    nombre_hospitalisation,
    nombre_deces,
    duree_hospitalisation_jours,
    score_satisfaction,
    nombre_prescriptions,
    duree_consultation_minutes
)
SELECT 
    -- Generate unique ID (add offset to avoid collision with consultations)
    ROW_NUMBER() OVER () + 2000000000 AS id_evenement,
    'HOSPITALISATION' AS type_evenement,
    
    -- Join to DIM_TEMPS - use date_entree
    t.id_temps,
    
    -- Patient
    p.id_patient,
    
    -- Diagnostic
    h."Code_diagnostic" AS code_diag,
    
    -- Professionnel (if available in hospitalisation data)
    NULL AS id_professionnel,
    
    -- Etablissement
    NULL AS id_etablissement,
    
    -- Mutuelle (try to get from patient's mutuelle)
    adh."Id_mut" AS id_mutuelle,
    
    -- Lieu (from patient postal code)
    p.code_postal AS code_lieu,
    
    -- Measures
    0 AS nombre_consultation,
    1 AS nombre_hospitalisation,
    0 AS nombre_deces,
    
    -- Calculate hospitalization duration (with safe conversion)
    TRY_CAST(h."Jour_Hospitalisation" AS INTEGER) as duree_hospitalisation_jours,
    
    NULL AS score_satisfaction,
    0 AS nombre_prescriptions,
    0 AS duree_consultation_minutes

FROM s3_files_views.hospitalisation h
LEFT JOIN hypercube.DIM_TEMPS t 
    ON TRY_CAST(h.date_entree AS DATE) = t.date_complete
LEFT JOIN hypercube.DIM_PATIENT p 
    ON h."Id_patient" = p.id_patient
LEFT JOIN adher adh 
    ON p.id_patient = adh."Id_patient"
WHERE TRY_CAST(h.date_entree AS DATE) IS NOT NULL;


-- =====================================================
-- 3. DECES (from S3 deces view)
-- =====================================================
INSERT INTO hypercube.FAIT_EVENEMENT_SANTE (
    id_evenement,
    type_evenement,
    id_temps,
    id_patient,
    code_diag,
    id_professionnel,
    id_etablissement,
    id_mutuelle,
    code_lieu,
    nombre_consultation,
    nombre_hospitalisation,
    nombre_deces,
    duree_hospitalisation_jours,
    score_satisfaction,
    nombre_prescriptions,
    duree_consultation_minutes
)
WITH deces_clean AS (
    SELECT 
        d.nom,
        d.prenom,
        d.date_naissance,
        d.code_lieu_deces,
        -- Reconstruit une date valide si on a juste l'année
        TRY_CAST(
            CASE 
                WHEN CAST(d.date_deces AS VARCHAR) ~ '^\d{4}$' 
                THEN CAST(d.date_deces AS VARCHAR) || '-01-01'
                ELSE CAST(d.date_deces AS VARCHAR)
            END AS DATE
        ) AS date_deces_valide
    FROM read_csv_auto('s3://bloc-big-data/DECES EN FRANCE/deces.csv', types={'date_naissance': 'VARCHAR', 'date_deces': 'VARCHAR'}) d
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY dc.date_deces_valide, dc.nom, dc.prenom) + 3000000000 AS id_evenement,
    'DECES' AS type_evenement,
    
    t.id_temps,
    p.id_patient,
    
    NULL AS code_diag,
    NULL AS id_professionnel,
    NULL AS id_etablissement,
    NULL AS id_mutuelle,
    
    -- Vérifie que le code_lieu existe dans DIM_LIEU, sinon NULL
    CASE 
        WHEN l.code_lieu IS NOT NULL THEN dc.code_lieu_deces
        ELSE NULL
    END AS code_lieu,
    
    0 AS nombre_consultation,
    0 AS nombre_hospitalisation,
    1 AS nombre_deces,
    0 AS duree_hospitalisation_jours,
    NULL AS score_satisfaction,
    0 AS nombre_prescriptions,
    0 AS duree_consultation_minutes
FROM deces_clean dc
LEFT JOIN hypercube.DIM_TEMPS t 
    ON dc.date_deces_valide = t.date_complete
LEFT JOIN hypercube.DIM_PATIENT p 
    ON UPPER(TRIM(dc.nom)) = UPPER(TRIM(p.nom))
    AND UPPER(TRIM(dc.prenom)) = UPPER(TRIM(p.prenom))
    AND dc.date_naissance = p.date_naissance
LEFT JOIN hypercube.DIM_LOCALISATION l
    ON dc.code_lieu_deces = l.code_lieu
WHERE dc.date_deces_valide IS NOT NULL;

-- =====================================================
-- 4. SATISFACTION SCORES (from S3 recueil view)
-- Link satisfaction scores directly from recueil view
-- =====================================================
INSERT INTO hypercube.FAIT_EVENEMENT_SANTE (
    id_evenement,
    type_evenement,
    id_temps,
    id_patient,
    code_diag,
    id_professionnel,
    id_etablissement,
    id_mutuelle,
    code_lieu,
    nombre_consultation,
    nombre_hospitalisation,
    nombre_deces,
    duree_hospitalisation_jours,
    score_satisfaction,
    nombre_prescriptions,
    duree_consultation_minutes
)
SELECT
    ROW_NUMBER() OVER () + 4000000000 AS id_evenement,
    'SATISFACTION' AS type_evenement,
    NULL AS id_temps,
    NULL AS id_patient,
    NULL AS code_diag,
    NULL AS id_professionnel,
    NULL AS id_etablissement,
    NULL AS id_mutuelle,
    NULL AS code_lieu,
    0 AS nombre_consultation,
    0 AS nombre_hospitalisation,
    0 AS nombre_deces,
    0 AS duree_hospitalisation_jours,
    r.score_all_rea_ajust AS score_satisfaction,
    0 AS nombre_prescriptions,
    0 AS duree_consultation_minutes
FROM s3_files_views.recueil r
WHERE r.score_all_rea_ajust IS NOT NULL;
