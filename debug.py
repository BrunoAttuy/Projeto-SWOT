#!/usr/bin/env python3
"""
Monitor diário para dados SWOT - VERSÃO CORRIGIDA
Usando período November 2023 que sabemos que tem dados
"""
import sys
import os
from datetime import datetime, timedelta
sys.path.append('src')

from core.swot_downloader import SWOTDownloader
from utils.config import get_regions
from utils.logger import setup_logger
from database.connection import DatabaseConnection
import tempfile
import xarray as xr
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import psycopg2
import earthaccess

def check_granule_exists(granule_name, db_connection):
    """Verificar se granule já existe no banco"""
    try:
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM granules WHERE granule_name = %s", (granule_name,))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except:
        return False

def extract_granule_name(granule):
    """Extrair nome do granule - VERSÃO CORRIGIDA"""
    try:
        # Usar o native-id como no código funcionando
        if isinstance(granule, dict) and 'meta' in granule and 'native-id' in granule['meta']:
            return granule['meta']['native-id']
        elif hasattr(granule, 'data_links'):
            from pathlib import Path
            filename = Path(granule.data_links()[0]).name
            return filename.replace('.nc', '')
        else:
            import hashlib
            granule_str = str(granule)
            return f"granule_{hashlib.md5(granule_str.encode()).hexdigest()[:8]}"
    except:
        return f"unknown_granule_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def search_swot_data_corrected(region, start_date, end_date):
    """Buscar dados SWOT usando método funcionando"""
    try:
        # Autenticar usando earthaccess
        auth = earthaccess.login()
        if not auth:
            print("   ERRO: Falha na autenticação earthaccess")
            return []
        
        # Converter bbox da região para formato correto
        bbox = region.get('bbox')
        if not bbox:
            print("   ERRO: Região sem bbox definido")
            return []
        
        # Formato correto: (min_lon, min_lat, max_lon, max_lat)
        aoi = (bbox[1], bbox[0], bbox[3], bbox[2])  # Converter de [min_lat, min_lon, max_lat, max_lon]
        
        # Buscar usando nome correto do produto
        results = earthaccess.search_data(
            short_name='SWOT_L2_HR_PIXC_2.0',  # ✅ NOME CORRETO
            temporal=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
            bounding_box=aoi
        )
        
        print(f"   Busca earthaccess: {len(results)} granules encontrados")
        return results
        
    except Exception as e:
        print(f"   ERRO na busca earthaccess: {e}")
        return []

def download_swot_data_corrected(granules, temp_dir):
    """Download usando earthaccess - método funcionando"""
    try:
        downloaded_files = earthaccess.download(granules, temp_dir)
        return downloaded_files
    except Exception as e:
        print(f"   ERRO no download earthaccess: {e}")
        return []

def process_netcdf_file_corrected(file_path, region):
    """Processar arquivo NetCDF - BASEADO NO CÓDIGO FUNCIONANDO"""
    try:
        print(f"   Processando arquivo: {file_path}")
        
        # Usar método exato do código funcionando
        ds_pixc = xr.open_dataset(file_path, group='pixel_cloud', engine='h5netcdf')
        
        # Criar GeoDataFrame como no código funcionando
        gdf = gpd.GeoDataFrame(
            data={
                'height': ds_pixc.height.values.astype('float32'),
                'classification': ds_pixc.classification.values.astype('uint8'),
                'coherent_power': ds_pixc.coherent_power.values.astype('float32'),
                'latitude': ds_pixc.latitude.values.astype('float32'),
                'longitude': ds_pixc.longitude.values.astype('float32'),
            },
            geometry=gpd.points_from_xy(
                ds_pixc.longitude.values,
                ds_pixc.latitude.values
            )
        )
        
        print(f"   GeoDataFrame criado com {len(gdf)} pixels")
        
        # Converter para DataFrame simples para compatibilidade
        df = pd.DataFrame({
            'latitude': gdf['latitude'],
            'longitude': gdf['longitude'], 
            'height': gdf['height'],
            'classification': gdf['classification'],
            'coherent_power': gdf['coherent_power']
        })
        
        # Remover valores inválidos
        df = df.dropna(subset=['latitude', 'longitude'])
        print(f"   Após limpeza: {len(df)} pixels válidos")
        
        # Filtrar por região se especificado
        if region and 'bbox' in region:
            bbox = region['bbox']
            mask = (
                (df['longitude'] >= bbox[1]) & (df['longitude'] <= bbox[3]) &
                (df['latitude'] >= bbox[0]) & (df['latitude'] <= bbox[2])
            )
            df = df[mask]
            print(f"   Após filtro regional: {len(df)} pixels")
        
        ds_pixc.close()
        return df
        
    except Exception as e:
        print(f"   ERRO processando NetCDF: {e}")
        import traceback
        traceback.print_exc()
        return None

def insert_granule_data(df, granule_name, region, db_connection):
    """Inserir dados no banco"""
    try:
        cursor = db_connection.cursor()
        
        # Inserir granule
        cursor.execute("""
            INSERT INTO granules (granule_name, mission_id, region_id, total_pixels, created_at)
            VALUES (%s, 1, %s, %s, %s)
            RETURNING granule_id
        """, (granule_name, region.get('id'), len(df), datetime.now()))
        
        granule_id = cursor.fetchone()[0]
        print(f"   Granule inserido com ID: {granule_id}")
        
        # Inserir pixels em lotes
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            insert_data = []
            for _, row in batch.iterrows():
                insert_data.append((
                    granule_id,
                    float(row['latitude']),
                    float(row['longitude']),
                    float(row.get('height', None)) if 'height' in row and pd.notna(row['height']) else None,
                    int(row.get('classification', None)) if 'classification' in row and pd.notna(row['classification']) else None,
                    float(row.get('coherent_power', None)) if 'coherent_power' in row and pd.notna(row['coherent_power']) else None,
                    datetime.now()
                ))
            
            cursor.executemany("""
                INSERT INTO pixel_data (granule_id, latitude, longitude, height_m, classification_id, coherent_power, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, insert_data)
            
            print(f"   Inserido lote {i//batch_size + 1}: {len(batch)} pixels")
        
        db_connection.commit()
        cursor.close()
        return True
        
    except Exception as e:
        print(f"   ERRO inserindo dados: {e}")
        db_connection.rollback()
        return False

def main():
    """Função principal do monitor - VERSÃO CORRIGIDA"""
    
    # Setup
    logger = setup_logger()
    print(f"MONITOR SWOT - NOVEMBER 2023 iniciado - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Conectar ao banco
        from dotenv import load_dotenv
        load_dotenv()
        
        db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),  
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        
        print("Conectado ao banco de dados")
        
        # Obter regiões ativas
        regions = get_regions()
        print(f"MONITORANDO {len(regions)} regiões")
        
        # USAR PERÍODO QUE SABEMOS QUE TEM DADOS (NOV 2023)
        start_date = datetime(2023, 11, 1).date()
        end_date = datetime(2023, 11, 30).date()
        
        print(f"PERÍODO: {start_date} até {end_date}")
        
        total_new_granules = 0
        
        # Processar cada região
        for region in regions:
            print(f"\nVERIFICANDO região: {region['name']}")
            
            # Buscar dados usando método corrigido
            results = search_swot_data_corrected(region, start_date, end_date)
            
            if not results:
                print(f"   INFO: Nenhum dado encontrado")
                continue
                
            print(f"   ENCONTRADOS: {len(results)} granules")
            
            # Verificar quais são novos
            new_granules = []
            for granule in results:
                granule_name = extract_granule_name(granule)
                if not check_granule_exists(granule_name, db_conn):
                    new_granules.append(granule)
            
            if not new_granules:
                print(f"   INFO: Nenhum dado novo ({len(results)} já processados)")
                continue
                
            print(f"   NOVOS: {len(new_granules)} novos granules encontrados")
            
            # Processar granules novos (limitando para teste)
            for granule in new_granules[:1]:  # Limitar a 1 por região para teste
                try:
                    granule_name = extract_granule_name(granule)
                    print(f"   PROCESSANDO: {granule_name}")
                    
                    # Download usando earthaccess
                    with tempfile.TemporaryDirectory() as temp_dir:
                        print(f"   Baixando para: {temp_dir}")
                        files = download_swot_data_corrected([granule], temp_dir)
                        
                        if files:
                            print(f"   Arquivo baixado: {files[0]}")
                            
                            # Processar arquivo
                            df = process_netcdf_file_corrected(files[0], region)
                            
                            if df is not None and len(df) > 0:
                                # Inserir no banco
                                if insert_granule_data(df, granule_name, region, db_conn):
                                    print(f"   SUCESSO: {len(df)} pixels inseridos")
                                    total_new_granules += 1
                                else:
                                    print(f"   ERRO: Falha inserindo dados")
                            else:
                                print(f"   AVISO: Nenhum pixel válido")
                        else:
                            print(f"   ERRO: Falha no download")
                            
                except Exception as e:
                    print(f"   ERRO: Erro processando granule: {e}")
                    continue
        
        # Resumo final
        if total_new_granules > 0:
            print(f"\nSUCESSO: Monitor completado! {total_new_granules} novos granules processados")
        else:
            print(f"\nINFO: Monitor completado - Nenhum dado novo encontrado")
        
        db_conn.close()
        
    except Exception as e:
        print(f"ERRO: Erro no monitor: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    print(f"\nFINALIZADO: Execução finalizada - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    input("Pressione Enter para fechar...")
    sys.exit(exit_code)