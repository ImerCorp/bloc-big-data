CREATE TABLE IF NOT EXISTS hypercube.DIM_DIAGNOSTIC (
    code_diag VARCHAR(50) PRIMARY KEY,
    diagnostic VARCHAR(255),
    categorie_diagnostic VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_dim_diagnostic_categorie ON hypercube.DIM_DIAGNOSTIC(categorie_diagnostic);

INSERT INTO hypercube.DIM_DIAGNOSTIC (
    code_diag,
    diagnostic,
    categorie_diagnostic
)
SELECT DISTINCT
    "Code_diag" AS code_diag,
    "Diagnostic" AS diagnostic,
    -- Extract category from diagnostic code (first 3 characters typically)
    SUBSTRING("Code_diag", 1, 3) AS categorie_diagnostic
FROM diagnostic
WHERE "Code_diag" IS NOT NULL;
