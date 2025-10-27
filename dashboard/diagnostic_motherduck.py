import os
import duckdb
from dotenv import dotenv_values,load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv('/app/.env')
# Or simply (since /app is the working directory)
load_dotenv()

# Configuration de la connexion MotherDuck
WRITE_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")

def diagnostic_complet():
    print("=== DIAGNOSTIC COMPLET MOTHERDUCK ===")
    
    try:
        # Connexion
        conn = duckdb.connect(f"md:?motherduck_token={WRITE_TOKEN}")
        print("OK Connexion etablie")
        
        # Attacher le lakehouse
        try:
            conn.execute("ATTACH 'md:lakehouse'")
            print("OK Lakehouse attache")
        except:
            print("INFO Lakehouse deja attache")
        
        # 1. Lister toutes les bases de données
        print("\n1. BASES DE DONNÉES DISPONIBLES:")
        result = conn.execute("SHOW DATABASES")
        databases = result.fetchall()
        for db in databases:
            print(f"   - {db[0]}")
        
        # 2. Explorer le lakehouse
        print("\n2. SCHÉMAS DANS LAKEHOUSE:")
        try:
            result = conn.execute("SHOW SCHEMAS FROM lakehouse")
            schemas = result.fetchall()
            for schema in schemas:
                print(f"   - {schema[0]}")
        except Exception as e:
            print(f"   ERREUR: {e}")
        
        # 3. Tables dans s3_files_views
        print("\n3. TABLES DANS S3_FILES_VIEWS:")
        try:
            result = conn.execute("SHOW TABLES FROM lakehouse.s3_files_views")
            tables = result.fetchall()
            for table in tables:
                print(f"   - {table[0]}")
        except Exception as e:
            print(f"   ERREUR: {e}")
        
        # 4. Tester chaque table individuellement
        print("\n4. TEST D'ACCES AUX TABLES:")
        tables_to_test = ["deces", "hospitalisation", "etablissement_sante", "professionnel_sante", "recueil"]
        
        for table in tables_to_test:
            print(f"\n   Test table: {table}")
            try:
                # Essayer de compter les lignes
                result = conn.execute(f"SELECT COUNT(*) FROM lakehouse.s3_files_views.{table}")
                count = result.fetchone()[0]
                print(f"      OK {count} lignes")
                
                # Essayer de voir la structure
                result = conn.execute(f"DESCRIBE lakehouse.s3_files_views.{table}")
                columns = result.fetchall()
                print(f"      {len(columns)} colonnes: {[col[0] for col in columns[:3]]}...")
                
            except Exception as e:
                print(f"      ERREUR: {e}")
        
        # 5. Vérifier les vues SQL
        print("\n5. DEFINITION DES VUES:")
        for table in tables_to_test:
            try:
                result = conn.execute(f"SHOW CREATE VIEW lakehouse.s3_files_views.{table}")
                view_def = result.fetchone()[0]
                print(f"\n   Vue {table}:")
                print(f"      {view_def[:100]}...")
            except Exception as e:
                print(f"   Impossible de voir la definition de {table}: {e}")
        
        # 6. Chercher des données alternatives
        print("\n6. RECHERCHE DE DONNEES ALTERNATIVES:")
        
        # Vérifier s'il y a des tables dans d'autres schémas
        try:
            result = conn.execute("SHOW TABLES FROM lakehouse.main")
            tables_main = result.fetchall()
            if tables_main:
                print("   Tables dans lakehouse.main:")
                for table in tables_main:
                    print(f"      - {table[0]}")
        except:
            print("   Pas de tables dans lakehouse.main")
        
        # Vérifier les données de démonstration
        try:
            conn.execute("USE demo_bigdata")
            result = conn.execute("SHOW TABLES")
            demo_tables = result.fetchall()
            if demo_tables:
                print("   Tables de demonstration disponibles:")
                for table in demo_tables:
                    print(f"      - {table[0]}")
        except:
            print("   Pas de tables de demonstration")
        
        conn.close()
        print("\n=== DIAGNOSTIC TERMINE ===")
        
    except Exception as e:
        print(f"ERREUR GENERALE: {e}")

if __name__ == "__main__":
    diagnostic_complet()
