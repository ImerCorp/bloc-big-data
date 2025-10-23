-- Secret for Nuremberg location
CREATE OR REPLACE SECRET hetzner_nbg1 IN MOTHERDUCK (
    TYPE S3,
    KEY_ID {{ s3_key_id }}, 
    SECRET {{ s3_secret }},
    ENDPOINT {{ s3_endpoint }},
    SCOPE {{ s3_scope }}
);

CREATE SCHEMA IF NOT EXISTS s3_files_views;
