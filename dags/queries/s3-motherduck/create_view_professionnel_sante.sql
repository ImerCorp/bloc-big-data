-- PROFESSIONNEL_SANTE
CREATE OR REPLACE VIEW {{ database_name }}.s3_files_views.professionnel_sante AS 
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
