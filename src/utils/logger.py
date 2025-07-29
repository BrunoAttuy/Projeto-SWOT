import logging
from pathlib import Path
from datetime import datetime

def setup_logger():
    # Criar pasta logs se não existir
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Nome do arquivo de log
    log_file = log_dir / f"swot_{datetime.now().strftime('%Y%m%d')}.log"
    
    # Configurar logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True
    )
    
    return logging.getLogger('swot')