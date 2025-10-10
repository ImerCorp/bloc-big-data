import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional


class SupabaseQueryExecutor:
    """Execute queries against Supabase Postgres database."""
    
    def __init__(self, host: str, database: str, user: str, password: str, port: str = '5432'):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
    
    def execute(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )
        
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, params)
            results = cur.fetchall()
            conn.commit()
            return [dict(row) for row in results]
        finally:
            cur.close()
            conn.close()
    
    def test_connection(self) -> bool:
        """Test if connection works."""
        try:
            self.execute("SELECT 1")
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
