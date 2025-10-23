-- ACTIVITE_PROFESSIONNEL_SANTE
CREATE OR REPLACE VIEW {{ database_name }}.s3_files_views.activite_professionnel_sante AS 
SELECT * FROM 's3://bloc-big-data/Etablissement de SANTE/activite_professionnel_sante.csv';
