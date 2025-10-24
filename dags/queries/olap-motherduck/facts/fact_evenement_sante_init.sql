CREATE TABLE IF NOT EXISTS hypercube.FAIT_EVENEMENT_SANTE (
    id_evenement BIGINT PRIMARY KEY,
    type_evenement VARCHAR(50) NOT NULL, -- 'CONSULTATION', 'HOSPITALISATION', 'DECES'
    
    -- Foreign keys to dimensions
    id_temps INTEGER,
    id_patient BIGINT,
    code_diag VARCHAR(50),
    id_professionnel VARCHAR(50),
    id_etablissement VARCHAR(50),
    id_mutuelle BIGINT,
    code_lieu VARCHAR(50),
    
    -- Measures (métriques additives)
    nombre_consultation INTEGER DEFAULT 0,
    nombre_hospitalisation INTEGER DEFAULT 0,
    nombre_deces INTEGER DEFAULT 0,
    duree_hospitalisation_jours INTEGER DEFAULT 0,
    score_satisfaction DECIMAL(5,2),
    nombre_prescriptions INTEGER DEFAULT 0,
    duree_consultation_minutes INTEGER DEFAULT 0,
    
    -- Foreign key constraints
    FOREIGN KEY (id_temps) REFERENCES hypercube.DIM_TEMPS(id_temps),
    FOREIGN KEY (id_patient) REFERENCES hypercube.DIM_PATIENT(id_patient),
    FOREIGN KEY (code_diag) REFERENCES hypercube.DIM_DIAGNOSTIC(code_diag),
    FOREIGN KEY (id_professionnel) REFERENCES hypercube.DIM_PROFESSIONNEL(identifiant),
    FOREIGN KEY (id_etablissement) REFERENCES hypercube.DIM_ETABLISSEMENT(identifiant_organisation),
    FOREIGN KEY (id_mutuelle) REFERENCES hypercube.DIM_MUTUELLE(id_mut),
    FOREIGN KEY (code_lieu) REFERENCES hypercube.DIM_LOCALISATION(code_lieu)
);

-- Indexes on fact table for query performance
CREATE INDEX IF NOT EXISTS idx_fait_type ON hypercube.FAIT_EVENEMENT_SANTE(type_evenement);
CREATE INDEX IF NOT EXISTS idx_fait_temps ON hypercube.FAIT_EVENEMENT_SANTE(id_temps);
CREATE INDEX IF NOT EXISTS idx_fait_patient ON hypercube.FAIT_EVENEMENT_SANTE(id_patient);
CREATE INDEX IF NOT EXISTS idx_fait_diag ON hypercube.FAIT_EVENEMENT_SANTE(code_diag);
CREATE INDEX IF NOT EXISTS idx_fait_prof ON hypercube.FAIT_EVENEMENT_SANTE(id_professionnel);
CREATE INDEX IF NOT EXISTS idx_fait_etab ON hypercube.DIM_ETABLISSEMENT(identifiant_organisation);
CREATE INDEX IF NOT EXISTS idx_fait_lieu ON hypercube.FAIT_EVENEMENT_SANTE(code_lieu);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fait_temps_etab ON hypercube.FAIT_EVENEMENT_SANTE(id_temps, id_etablissement);
CREATE INDEX IF NOT EXISTS idx_fait_temps_diag ON hypercube.FAIT_EVENEMENT_SANTE(id_temps, code_diag);
CREATE INDEX IF NOT EXISTS idx_fait_temps_patient ON hypercube.FAIT_EVENEMENT_SANTE(id_temps, id_patient);
