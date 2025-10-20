CREATE TABLE IF NOT EXISTS hypercube.DIM_ETABLISSEMENT (
    identifiant_organisation VARCHAR(50) PRIMARY KEY,
    raison_sociale VARCHAR(255),
    adresse VARCHAR(255),
    commune VARCHAR(100),
    code_postal VARCHAR(10),
    pays VARCHAR(50),
    finess VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_dim_etab_commune ON hypercube.DIM_ETABLISSEMENT(commune);

INSERT INTO hypercube.DIM_ETABLISSEMENT (
    identifiant_organisation,
    raison_sociale,
    adresse,
    commune,
    code_postal,
    pays,
    finess
)
SELECT DISTINCT
    identifiant_organisation,
    raison_sociale_site,
    adresse,
    commune,
    code_postal,
    pays,
    finess_etablissement_juridique
FROM s3_files_views.etablissement_sante
WHERE identifiant_organisation IS NOT NULL;
