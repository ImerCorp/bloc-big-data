CREATE TABLE IF NOT EXISTS hypercube.DIM_MUTUELLE (
    id_mut BIGINT PRIMARY KEY,
    nom VARCHAR(255),
    adresse VARCHAR(255)
);

INSERT INTO hypercube.DIM_MUTUELLE (
    id_mut,
    nom,
    adresse
)
SELECT DISTINCT
    "Id_Mut" AS id_mut,
    "Nom" AS nom,
    "Adresse" AS adresse
FROM lakehouse.main.mutuelle
WHERE "Id_Mut" IS NOT NULL;
