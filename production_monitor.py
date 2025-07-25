#!/usr/bin/env python3
"""
Monitor SWOT - Versão de Produção
Configurado para suas regiões específicas
"""
import sys
import os
from datetime import datetime, timedelta
sys.path.append('src')

from core.swot_downloader import SWOTDownloader
from utils.config import get_regions
from utils.logger import setup_logger
import psycopg2
import tempfile
import xarray as xr
import pandas as pd

# CONFIGURAÇÕES DE PRODUÇÃO
MAX_GRANULES_PER_REGION = 2  # Máximo por região
MAX_EXECUTION_TIME_MINUTES = 45  # Timeout
MAX_PIXELS_PER_GRANULE = 200000  # Pular granules muito grandes

def process_region_optimized(region, downloader, db_conn):
    """Processar região de forma otimizada"""
    
    print(f"\n Processando: {region['name']}")
    
    # Buscar dados
    results = downloader.search_data(region)
    
    if not results:
        print(f"    Nenhum dado encontrado")
        return 0
    
    print(f"    Encontrados: {len(results)} granules")
    
    # Verificar granules novos
    new_granules = []
    for granule in results[:MAX_GRANULES_PER_REGION]:  # Limitar
        granule_name = extract_granule_name(granule)
        if not check_granule_exists(granule_name, db_conn):
            new_granules.append(granule)
    
    if not new_granules:
        print(f"    Todos os granules já processados")
        return 0
    
    print(f"    Novos: {len(new_granules)} granules")
    
    processed = 0
    
    # Processar granules novos
    for granule in new_granules:
        try:
            granule_name = extract_granule_name(granule)
            print(f"    {granule_name[:30]}...")
            
            # Download e processamento
            with tempfile.TemporaryDirectory() as temp_dir:
                files = downloader.download_data([granule], temp_dir)
                
                if files:
                    df = process_netcdf_quick(files[0], region)
                    
                    if df is not None and len(df) > 0:
                        # Pular se muito grande
                        if len(df) > MAX_PIXELS_PER_GRANULE:
                            print(f"    Pulando (muito grande: {len(df)} pixels)")
                            continue
                        
                        # Inserir no banco
                        if insert_granule_data_optimized(df, granule_name, region, db_conn):
                            print(f"    {len(df)} pixels inseridos")
                            processed += 1
                        else:
                            print(f"    Erro na inserção")
                    else:
                        print(f"    Nenhum pixel válido")
                        
        except Exception as e:
            print(f"    Erro: {str(e)[:50]}...")
            continue
    
    return processed

def process_netcdf_quick(file_path, region):
    """Processamento rápido de NetCDF"""
    try:
        with xr.open_dataset(file_path, group='pixel_cloud', engine='h5netcdf') as ds:
            # Extrair apenas coordenadas e classificação
            data = {
                'latitude': ds.latitude.values.astype('float32'),
                'longitude': ds.longitude.values.astype('float32'),
            }
            
            # Adicionar height e classification se existir
            if 'height' in ds.variables:
                data['height'] = ds.height.values.astype('float32')
            if 'classification' in ds.variables:
                data['classification'] = ds.classification.values.astype('uint8')
            
            df = pd.DataFrame(data)
            df = df.dropna(subset=['latitude', 'longitude'])
            
            # Filtro regional
            if region and 'bbox' in region:
                bbox = region['bbox']
                mask = (
                    (df['longitude'] >= bbox[0]) & (df['longitude'] <= bbox[2]) &
                    (df['latitude'] >= bbox[1]) & (df['latitude'] <= bbox[3])
                )
                df = df[mask]
            
            return df
            
    except Exception as e:
        print(f"    Erro NetCDF: {e}")
        return None

def insert_granule_data_optimized(df, granule_name, region, db_connection):
    """Inserção otimizada no banco"""
    try:
        cursor = db_connection.cursor()
        
        # Inserir granule
        cursor.execute("""
            INSERT INTO granules (granule_name, mission_id, region_id, total_pixels, created_at)
            VALUES (%s, 1, %s, %s, %s)
            RETURNING granule_id
        """, (granule_name, region.get('id'), len(df), datetime.now()))
        
        granule_id = cursor.fetchone()[0]
        
        # Inserir pixels em lotes maiores
        batch_size = 2000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            insert_data = []
            for _, row in batch.iterrows():
                insert_data.append((
                    granule_id,
                    float(row['latitude']),
                    float(row['longitude']),
                    float(row.get('height', None)) if 'height' in row and pd.notna(row['height']) else None,
                    int(row.get('classification', None)) if 'classification' in row and pd.notna(row['classification']) and row['classification'] <= 7 else None,
                    datetime.now()
                ))
            
            cursor.executemany("""
                INSERT INTO pixel_data (granule_id, latitude, longitude, height_m, classification_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, insert_data)
        
        db_connection.commit()
        cursor.close()
        return True
        
    except Exception as e:
        print(f"    Erro inserção: {e}")
        db_connection.rollback()
        return False

def check_granule_exists(granule_name, db_connection):
    """Verificar se granule existe"""
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

def main():
    """Função principal otimizada"""
    
    start_time = datetime.now()
    logger = setup_logger()
    
    print(f" MONITOR SWOT PRODUÇÃO - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Timeout configurado: {MAX_EXECUTION_TIME_MINUTES} minutos")
    
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
        
        # Obter regiões
        regions = get_regions()
        active_regions = [r for r in regions if r.get('active', True)]
        
        print(f" Processando {len(active_regions)} regiões ativas")
        
        total_processed = 0
        
        # Processar cada região
        for region in active_regions:
            # Verificar timeout
            elapsed = datetime.now() - start_time
            if elapsed.total_seconds() > (MAX_EXECUTION_TIME_MINUTES * 60):
                print(f"⏱️ Timeout atingido ({MAX_EXECUTION_TIME_MINUTES}min)")
                break
            
            try:
                processed = process_region_optimized(region, downloader, db_conn)
                total_processed += processed
                
            except Exception as e:
                print(f" Erro na região {region['name']}: {e}")
                continue
        
        # Resumo final
        execution_time = datetime.now() - start_time
        
        print(f"\n EXECUÇÃO CONCLUÍDA:")
        print(f"    {total_processed} granules processados")
        print(f"    Tempo de execução: {execution_time}")
        print(f"    {len(active_regions)} regiões verificadas")
        
        db_conn.close()
        
        return 0
        
    except Exception as e:
        print(f" ERRO CRÍTICO: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    print(f"\nFINALIZADO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(exit_code)