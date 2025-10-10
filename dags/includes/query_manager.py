from pathlib import Path
from jinja2 import Template, UndefinedError

class QueryManager:
    def __init__(self, queries_directory: str = "queries"):
        self.queries_directory = Path(queries_directory)
        if not self.queries_directory.exists():
            raise ValueError(f"Queries directory '{queries_directory}' does not exist")
    def get_query(self, file_path: str) -> str:
        if Path(file_path).is_absolute():
            query_file = Path(file_path)
        else:
            query_file = self.queries_directory / file_path
        if not query_file.exists():
            raise FileNotFoundError(f"Query file '{query_file}' not found")
        try:
            with open(query_file, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception as e:
            raise IOError(f"Error reading query file '{query_file}': {str(e)}")
    def get_query_params(self, file_path: str, parameters: dict[str, ]) -> str:
        query = self.get_query(file_path)
        try:
            template = Template(query)
            return template.render(**parameters)
        except UndefinedError as e:
            raise UndefinedError(f"Missing parameter in template: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error rendering template: {str(e)}")