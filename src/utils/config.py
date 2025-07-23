import json
import os
from pathlib import Path

def load_config():
    """Carregar configurações do projeto"""
    config_file = Path('config/regions.json')
    
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        return {"regions": []}

def get_regions():
    """Obter regiões ativas"""
    config = load_config()
    return [r for r in config.get('regions', []) if r.get('active', True)]