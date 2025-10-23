-- ETABLISSEMENT_SANTE
CREATE OR REPLACE VIEW {{ database_name }}.s3_files_views.etablissement_sante AS 
SELECT * FROM 's3://bloc-big-data/Etablissement de SANTE/etablissement_sante.csv';
