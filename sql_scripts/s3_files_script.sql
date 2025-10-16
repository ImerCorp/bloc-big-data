-- Secret for Nuremberg location
CREATE OR REPLACE SECRET hetzner_nbg1 IN MOTHERDUCK (
    TYPE S3,
    KEY_ID '', 
    SECRET '',
    ENDPOINT '',
    SCOPE 's3://bloc-big-data'
);

CREATE OR REPLACE SCHEMA IF NOT EXISTS s3_files_views;

-- DECES
CREATE OR REPLACE VIEW s3_files_views.deces AS 
SELECT * FROM 's3://bloc-big-data/DECES EN FRANCE/deces.csv';

-- HOSPITALISATION
CREATE OR REPLACE VIEW s3_files_views.hospitalisation AS 
SELECT * FROM 's3://bloc-big-data/Hospitalisation/Hospitalisations.csv';

-- RECUEIL
CREATE OR REPLACE VIEW s3_files_views.recueil AS 
WITH recueil_temp AS (
    SELECT *
    FROM read_csv('s3://bloc-big-data/Satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv',
        delim = '；',
        header = true,
        encoding = 'IBM_1252',
        columns = {
            'finess': 'VARCHAR',
            'rs_finess': 'VARCHAR',
            'finess_geo': 'VARCHAR',
            'rs_finess_geo': 'VARCHAR',
            'region': 'VARCHAR',
            'participation': 'VARCHAR',
            'Depot': 'VARCHAR',
            'nb_rep_score_all_rea_ajust': 'VARCHAR',
            'score_all_rea_ajust': 'VARCHAR',
            'classement': 'VARCHAR',
            'evolution': 'VARCHAR',
            'nb_rep_score_accueil_rea_ajust': 'VARCHAR',
            'score_accueil_rea_ajust': 'VARCHAR',
            'nb_rep_score_PECinf_rea_ajust': 'VARCHAR',
            'score_PECinf_rea_ajust': 'VARCHAR',
            'nb_rep_score_PECmed_rea_ajust': 'VARCHAR',
            'score_PECmed_rea_ajust': 'VARCHAR',
            'nb_rep_score_chambre_rea_ajust': 'VARCHAR',
            'score_chambre_rea_ajust': 'VARCHAR',
            'nb_rep_score_repas_rea_ajust': 'VARCHAR',
            'score_repas_rea_ajust': 'VARCHAR',
            'nb_rep_score_sortie_rea_ajust': 'VARCHAR',
            'score_sortie_rea_ajust': 'VARCHAR'
        }
    )
),
normalized AS (
    SELECT
        -- Convert full-width to half-width characters using TRANSLATE
        TRANSLATE(finess, 
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oe') AS finess,
        TRANSLATE(rs_finess, 
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oe') AS rs_finess,
        TRANSLATE(finess_geo, 
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oe') AS finess_geo,
        TRANSLATE(rs_finess_geo, 
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oe') AS rs_finess_geo,
        TRANSLATE(region, 
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oe') AS region,
        TRANSLATE(participation, 
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oe') AS participation,
        TRANSLATE(Depot, 
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oe') AS Depot,
        
        -- Numeric columns - convert and cast
        TRY_CAST(TRANSLATE(nb_rep_score_all_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS INTEGER) AS nb_rep_score_all_rea_ajust,
        TRY_CAST(TRANSLATE(score_all_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS DOUBLE) AS score_all_rea_ajust,
        TRANSLATE(classement, '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪ', '0123456789ABCDEFGHIJ') AS classement,
        TRANSLATE(evolution, '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oe') AS evolution,
        
        TRY_CAST(TRANSLATE(nb_rep_score_accueil_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS INTEGER) AS nb_rep_score_accueil_rea_ajust,
        TRY_CAST(TRANSLATE(score_accueil_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS DOUBLE) AS score_accueil_rea_ajust,
        
        TRY_CAST(TRANSLATE(nb_rep_score_PECinf_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS INTEGER) AS nb_rep_score_PECinf_rea_ajust,
        TRY_CAST(TRANSLATE(score_PECinf_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS DOUBLE) AS score_PECinf_rea_ajust,
        
        TRY_CAST(TRANSLATE(nb_rep_score_PECmed_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS INTEGER) AS nb_rep_score_PECmed_rea_ajust,
        TRY_CAST(TRANSLATE(score_PECmed_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS DOUBLE) AS score_PECmed_rea_ajust,
        
        TRY_CAST(TRANSLATE(nb_rep_score_chambre_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS INTEGER) AS nb_rep_score_chambre_rea_ajust,
        TRY_CAST(TRANSLATE(score_chambre_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS DOUBLE) AS score_chambre_rea_ajust,
        
        TRY_CAST(TRANSLATE(nb_rep_score_repas_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS INTEGER) AS nb_rep_score_repas_rea_ajust,
        TRY_CAST(TRANSLATE(score_repas_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS DOUBLE) AS score_repas_rea_ajust,
        
        TRY_CAST(TRANSLATE(nb_rep_score_sortie_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS INTEGER) AS nb_rep_score_sortie_rea_ajust,
        TRY_CAST(TRANSLATE(score_sortie_rea_ajust, '０１２３４５６７８９．', '0123456789.') AS DOUBLE) AS score_sortie_rea_ajust
        
    FROM recueil_temp
)
SELECT * FROM normalized;

CREATE OR REPLACE VIEW s3_files_views.lexique AS 
WITH lexique_temp AS (
    SELECT *
    FROM read_csv('s3://bloc-big-data/Satisfaction/ESATIS48H_MCO_recueil2017_lexique.csv',
        delim = '；',
        encoding = 'IBM_1252',
        columns = {
            'name': 'VARCHAR',
            'label': 'VARCHAR'
        }
    )
)
SELECT
    TRANSLATE(name, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ＿－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-''oeeaucaeiueiuEEAUCAEIUEIU') AS name,
    TRANSLATE(label, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ＿－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-''oeeaucaeiueiuEEAUCAEIUEIU') AS label
FROM lexique_temp;

-- PROFESSIONNEL_SANTE
CREATE OR REPLACE VIEW s3_files_views.professionnel_sante AS 
WITH professionnel_temp AS (
    SELECT * 
    FROM read_csv('s3://bloc-big-data/Etablissement de SANTE/professionnel_sante.csv',
        delim = '；',
        encoding = 'IBM_1252',
        columns = {
            'identifiant': 'VARCHAR',
            'civilite': 'VARCHAR',
            'categorie_professionnelle': 'VARCHAR',
            'nom': 'VARCHAR',
            'prenom': 'VARCHAR',
            'commune': 'VARCHAR',
            'profession': 'VARCHAR',
            'specialite': 'VARCHAR',
            'type_identifiant': 'VARCHAR'
        }
    )
)
SELECT
    TRANSLATE(identifiant, 
        '０１２３４５６７８９', 
        '0123456789') AS identifiant,
    TRANSLATE(civilite, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oeeaucaeiueiuEEAUCAEIUEIU') AS civilite,
    TRANSLATE(categorie_professionnelle, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oeeaucaeiueiuEEAUCAEIUEIU') AS categorie_professionnelle,
    TRANSLATE(nom, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oeeaucaeiueiuEEAUCAEIUEIU') AS nom,
    TRANSLATE(prenom, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oeeaucaeiueiuEEAUCAEIUEIU') AS prenom,
    TRANSLATE(commune, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oeeaucaeiueiuEEAUCAEIUEIU') AS commune,
    TRANSLATE(profession, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oeeaucaeiueiuEEAUCAEIUEIU') AS profession,
    TRANSLATE(specialite, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oeeaucaeiueiuEEAUCAEIUEIU') AS specialite,
    TRANSLATE(type_identifiant, 
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－', 
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-''oeeaucaeiueiuEEAUCAEIUEIU') AS type_identifiant
FROM professionnel_temp;

-- ETABLISSEMENT_SANTE
CREATE OR REPLACE VIEW s3_files_views.etablissement_sante AS 
SELECT * FROM 's3://bloc-big-data/Etablissement de SANTE/etablissement_sante.csv';

-- ACTIVITE_PROFESSIONNEL_SANTE
CREATE OR REPLACE VIEW s3_files_views.activite_professionnel_sante AS 
SELECT * FROM 's3://bloc-big-data/Etablissement de SANTE/activite_professionnel_sante.csv';
