CREATE TABLE IF NOT EXISTS hypercube.DIM_LOCALISATION (
    code_lieu VARCHAR(50) PRIMARY KEY,
    lieu VARCHAR(255),
    commune VARCHAR(100),
    pays VARCHAR(50)
);
CREATE INDEX IF NOT EXISTS idx_dim_loc_commune ON hypercube.DIM_LOCALISATION(commune);

-- =====================================================
-- Delete DIM_LOCALISATION data
-- =====================================================

DELETE FROM hypercube.DIM_LOCALISATION;

INSERT INTO hypercube.DIM_LOCALISATION (
    code_lieu,
    lieu,
    commune,
    pays
)
SELECT 
    code_lieu,
    MAX(lieu) AS lieu,
    MAX(commune) AS commune,
    MAX(pays) AS pays
FROM (
    -- From patient cities
    SELECT DISTINCT
        "Code_postal" AS code_lieu,
        "Ville" AS lieu,
        "Ville" AS commune,
        "Pays" AS pays
    FROM lakehouse.main.patient
    WHERE "Code_postal" IS NOT NULL AND "Ville" IS NOT NULL
    
    UNION
    
    -- From etablissements
    SELECT DISTINCT
        code_postal AS code_lieu,
        commune AS lieu,
        commune,
        pays
    FROM lakehouse.s3_files_views.etablissement_sante
    WHERE code_postal IS NOT NULL AND commune IS NOT NULL
    
    UNION
    
    -- From professionnel_sante (communes)
    SELECT DISTINCT
        commune AS code_lieu,
        commune AS lieu,
        commune,
        'FR' AS pays
    FROM lakehouse.s3_files_views.professionnel_sante
    WHERE commune IS NOT NULL
) combined
GROUP BY code_lieu;
