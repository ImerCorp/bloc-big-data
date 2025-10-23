-- DECES
CREATE OR REPLACE VIEW {{ database_name }}.s3_files_views.deces AS 
SELECT * FROM 's3://bloc-big-data/DECES EN FRANCE/deces.csv';
