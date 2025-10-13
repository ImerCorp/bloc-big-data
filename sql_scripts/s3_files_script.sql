-- Secret for Nuremberg location
CREATE OR REPLACE SECRET hetzner_nbg1 IN MOTHERDUCK (
    TYPE S3,
    KEY_ID '', 
    SECRET '',
    ENDPOINT '',
    SCOPE 's3://bloc-big-data'
);

CREATE OR REPLACE SCHEMA IF NOT EXISTS s3_files_views;

CREATE OR REPLACE VIEW s3_files_views.deces_materialized AS 
SELECT * FROM 's3://bloc-big-data/DECES EN FRANCE/deces.csv';
