CREATE OR REPLACE VIEW {{ database_name }}.s3_files_views.lexique AS 
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
