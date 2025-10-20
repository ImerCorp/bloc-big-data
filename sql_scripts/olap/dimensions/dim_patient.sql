-- Dim Patient
CREATE TABLE IF NOT EXISTS hypercube.DIM_PATIENT (
    id_patient BIGINT PRIMARY KEY,
    nom VARCHAR(50),
    prenom VARCHAR(50),
    sexe VARCHAR(10),
    age INTEGER,
    groupe_sanguin VARCHAR(10),
    ville VARCHAR(100),
    code_postal VARCHAR(10),
    pays VARCHAR(50),
    date_naissance DATE
);

CREATE INDEX IF NOT EXISTS idx_dim_patient_sexe ON hypercube.DIM_PATIENT(sexe);
CREATE INDEX IF NOT EXISTS idx_dim_patient_age ON hypercube.DIM_PATIENT(age);
CREATE INDEX IF NOT EXISTS idx_dim_patient_ville ON hypercube.DIM_PATIENT(ville);

INSERT INTO hypercube.DIM_PATIENT (
    id_patient,
    nom,
    prenom,
    sexe,
    age,
    groupe_sanguin,
    ville,
    code_postal,
    pays,
    date_naissance
)
SELECT DISTINCT
    "Id_patient" AS id_patient,
    "Nom" AS nom,
    "Prenom" AS prenom,
    "Sexe" AS sexe,
    "Age" AS age,
    "Groupe_sanguin" AS groupe_sanguin,
    "Ville" AS ville,
    "Code_postal" AS code_postal,
    "Pays" AS pays,
    -- Calculate date_naissance from Age if Date is birth date
    -- Otherwise, use a calculation based on current year - age
    TRY_CAST("Date" AS DATE) AS date_naissance
FROM patient
WHERE "Id_patient" IS NOT NULL;
