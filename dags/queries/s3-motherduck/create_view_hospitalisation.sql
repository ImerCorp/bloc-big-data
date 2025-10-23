-- HOSPITALISATION
CREATE OR REPLACE VIEW {{ database_name }}.s3_files_views.hospitalisation AS 
SELECT * FROM 's3://bloc-big-data/Hospitalisation/Hospitalisations.csv';
