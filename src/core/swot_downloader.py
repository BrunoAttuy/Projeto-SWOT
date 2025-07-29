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
        """Buscar dados SWOT para uma região - VERSÃO CORRIGIDA"""
        if not self.authenticate():
            return []
        
        try:
            
            start_date = '2023-11-01'
            end_date = '2023-11-30'
            
            print(f"   Buscando dados de {start_date} ate {end_date}")
            
            
            bbox = region['bbox']
            
           
            results = earthaccess.search_data(
                short_name='SWOT_L2_HR_PIXC_2.0',  
                bounding_box=(bbox[0], bbox[1], bbox[2], bbox[3]),  
                temporal=(start_date, end_date)
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