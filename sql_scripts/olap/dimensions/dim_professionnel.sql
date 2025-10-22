CREATE TABLE IF NOT EXISTS hypercube.DIM_PROFESSIONNEL (
    identifiant VARCHAR(50) PRIMARY KEY,
    nom VARCHAR(100),
    prenom VARCHAR(100),
    civilite VARCHAR(20),
    profession VARCHAR(100),
    specialite VARCHAR(100),
    code_specialite VARCHAR(50),
    categorie_professionnelle VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_dim_prof_profession ON hypercube.DIM_PROFESSIONNEL(profession);
CREATE INDEX IF NOT EXISTS idx_dim_prof_specialite ON hypercube.DIM_PROFESSIONNEL(specialite);

INSERT INTO hypercube.DIM_PROFESSIONNEL (
    identifiant,
    nom,
    prenom,
    civilite,
    profession,
    specialite,
    code_specialite,
    categorie_professionnelle
)
SELECT DISTINCT
    COALESCE(ps."Identifiant", s3.identifiant) AS identifiant,
    COALESCE(ps."Nom", s3.nom) AS nom,
    COALESCE(ps."Prenom", s3.prenom) AS prenom,
    COALESCE(ps."Civilite", s3.civilite) AS civilite,
    COALESCE(ps."Profession", s3.profession) AS profession,
    COALESCE(s.specialite, s3.specialite) AS specialite,
    COALESCE(ps."Code_specialite", s.code_specialite) AS code_specialite,
    COALESCE(ps."Categorie_professionnelle", s3.categorie_professionnelle) AS categorie_professionnelle
FROM lakehouse.main.professionnel_de_sante ps
FULL OUTER JOIN lakehouse.s3_files_views.professionnel_sante s3 
    ON ps."Identifiant" = s3.identifiant
LEFT JOIN lakehouse.main.specialites s 
    ON ps."Code_specialite" = s."Code_specialite"
WHERE COALESCE(ps."Identifiant", s3.identifiant) IS NOT NULL;
