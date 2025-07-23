import earthaccess
from datetime import datetime, timedelta
import logging

class SWOTDownloader:
    def __init__(self):
        self.logger = logging.getLogger('swot')
        self.auth = None
    
    def authenticate(self):
        """Autenticar com NASA Earthdata"""
        try:
            self.auth = earthaccess.login()
            if self.auth:
                self.logger.info("Autenticado com NASA Earthdata")
                return True
            else:
                self.logger.error("Falha na autenticacao")
                return False
        except Exception as e:
            self.logger.error(f"Erro na autenticacao: {e}")
            return False
    
    def search_data(self, region, days_back=2):
        """Buscar dados SWOT para uma região"""
        if not self.authenticate():
            return []
        
        try:
            # Definir período
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            
            # Extrair bbox da região [min_lon, min_lat, max_lon, max_lat]
            bbox = region['bbox']
            
            # Buscar dados - formato correto (min_lat, min_lon, max_lat, max_lon)
            results = earthaccess.search_data(
                short_name='SWOT_L2_HR_PIXC',
                bounding_box=(bbox[1], bbox[0], bbox[3], bbox[2]),
                temporal=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            )
            
            self.logger.info(f"Encontrados {len(results)} granules para {region['name']}")
            return results
            
        except Exception as e:
            self.logger.error(f"Erro na busca: {e}")
            return []
    
    def download_data(self, results, output_dir='data/raw'):
        """Download dos dados"""
        try:
            if not results:
                return []
            
            files = earthaccess.download(results, output_dir)
            self.logger.info(f"Baixados {len(files)} arquivos")
            return files
            
        except Exception as e:
            self.logger.error(f"Erro no download: {e}")
            return []