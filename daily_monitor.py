#!/usr/bin/env python3
"""
Monitor diário para dados SWOT
Verifica automaticamente se há novos dados nas regiões configuradas
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
    """Extrair nome do granule"""
    try:
        if hasattr(granule, 'data_links'):
            from pathlib import Path
            filename = Path(granule.data_links()[0]).name
            return filename.replace('.nc', '')
        else:
            import hashlib
            granule_str = str(granule)
            return f"granule_{hashlib.md5(granule_str.encode()).hexdigest()[:8]}"
    except:
        return f"unknown_granule_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def process_netcdf_file(file_path, region):
    """Processar arquivo NetCDF"""
    try:
        with xr.open_dataset(file_path) as ds:
            # Extrair dados básicos
            if 'latitude' not in ds.variables or 'longitude' not in ds.variables:
                return None
            
            # Criar DataFrame
            data = {
                'latitude': ds['latitude'].values.flatten(),
                'longitude': ds['longitude'].values.flatten()
            }
            
            # Adicionar outras variáveis se existirem
            optional_vars = ['height', 'classification', 'coherent_power']
            for var in optional_vars:
                if var in ds.variables:
                    data[var] = ds[var].values.flatten()
            
            df = pd.DataFrame(data)
            
            # Remover valores inválidos
            df = df.dropna(subset=['latitude', 'longitude'])
            
            # Filtrar por região
            if region and 'bbox' in region:
                bbox = region['bbox']
                mask = (
                    (df['longitude'] >= bbox[0]) & (df['longitude'] <= bbox[2]) &
                    (df['latitude'] >= bbox[1]) & (df['latitude'] <= bbox[3])
                )
                df = df[mask]
            
            return df
            
    except Exception as e:
        print(f"ERRO processando NetCDF: {e}")
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
        
        db_connection.commit()
        cursor.close()
        return True
        
    except Exception as e:
        print(f"ERRO inserindo dados: {e}")
        db_connection.rollback()
        return False

def main():
    """Função principal do monitor"""
    
    # Setup
    logger = setup_logger()
    print(f"MONITOR SWOT iniciado - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
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
        
        # Inicializar downloader
        downloader = SWOTDownloader()
        
        # Obter regiões ativas
        regions = get_regions()
        print(f"MONITORANDO {len(regions)} regioes")
        
        total_new_granules = 0
        
        # Processar cada região
        for region in regions:
            print(f"\nVERIFICANDO regiao: {region['name']}")
            
            # Buscar novos dados
            results = downloader.search_data(region, days_back=2)
            
            if not results:
                print(f"   INFO: Nenhum dado encontrado")
                continue
            
            # Verificar quais são novos
            new_granules = []
            for granule in results:
                granule_name = extract_granule_name(granule)
                if not check_granule_exists(granule_name, db_conn):
                    new_granules.append(granule)
            
            if not new_granules:
                print(f"   INFO: Nenhum dado novo ({len(results)} ja processados)")
                continue
            
            print(f"   NOVOS: {len(new_granules)} novos granules encontrados")
            
            # Processar granules novos
            for granule in new_granules[:3]:  # Limitar a 3 por região
                try:
                    granule_name = extract_granule_name(granule)
                    print(f"   PROCESSANDO: {granule_name}")
                    
                    # Download temporário
                    with tempfile.TemporaryDirectory() as temp_dir:
                        files = downloader.download_data([granule], temp_dir)
                        
                        if files:
                            # Processar arquivo
                            df = process_netcdf_file(files[0], region)
                            
                            if df is not None and len(df) > 0:
                                # Inserir no banco
                                if insert_granule_data(df, granule_name, region, db_conn):
                                    print(f"   SUCESSO: {len(df)} pixels inseridos")
                                    total_new_granules += 1
                                else:
                                    print(f"   ERRO: Falha inserindo dados")
                            else:
                                print(f"   AVISO: Nenhum pixel valido")
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
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    print(f"\nFINALIZADO: Execucao finalizada - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    input("Pressione Enter para fechar...")
    sys.exit(exit_code)