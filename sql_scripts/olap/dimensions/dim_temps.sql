CREATE TABLE IF NOT EXISTS hypercube.DIM_TEMPS (
    id_temps INTEGER PRIMARY KEY,
    date_complete DATE NOT NULL,
    annee INTEGER NOT NULL,
    mois INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    jour INTEGER NOT NULL,
    nom_mois VARCHAR(20),
    jour_semaine VARCHAR(20),
    semaine_annee INTEGER
);

INSERT INTO hypercube.DIM_TEMPS (
    id_temps,
    date_complete,
    annee,
    mois,
    trimestre,
    jour,
    nom_mois,
    jour_semaine,
    semaine_annee
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY d) AS id_temps,
    d AS date_complete,
    EXTRACT(YEAR FROM d) AS annee,
    EXTRACT(MONTH FROM d) AS mois,
    EXTRACT(QUARTER FROM d) AS trimestre,
    EXTRACT(DAY FROM d) AS jour,
    CASE EXTRACT(MONTH FROM d)
        WHEN 1 THEN 'Janvier'
        WHEN 2 THEN 'Février'
        WHEN 3 THEN 'Mars'
        WHEN 4 THEN 'Avril'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juillet'
        WHEN 8 THEN 'Août'
        WHEN 9 THEN 'Septembre'
        WHEN 10 THEN 'Octobre'
        WHEN 11 THEN 'Novembre'
        WHEN 12 THEN 'Décembre'
    END AS nom_mois,
    CASE EXTRACT(DAYOFWEEK FROM d)
        WHEN 0 THEN 'Dimanche'
        WHEN 1 THEN 'Lundi'
        WHEN 2 THEN 'Mardi'
        WHEN 3 THEN 'Mercredi'
        WHEN 4 THEN 'Jeudi'
        WHEN 5 THEN 'Vendredi'
        WHEN 6 THEN 'Samedi'
    END AS jour_semaine,
    EXTRACT(WEEK FROM d) AS semaine_annee
FROM (
    SELECT DATE '2015-01-01' + INTERVAL (n) DAY AS d
    FROM generate_series(0, 3652) AS t(n)  -- 10 years of dates
) dates;
