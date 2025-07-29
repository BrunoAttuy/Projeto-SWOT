#!/usr/bin/env python3
"""
Monitor SWOT - Versão de Produção CORRIGIDA
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
MAX_GRANULES_PER_REGION = 5  # Máximo por região
MAX_EXECUTION_TIME_MINUTES = 45  # Timeout
MAX_PIXELS_PER_GRANULE = 500000  # Pular granules muito grandes

def process_region_optimized(region, downloader, db_conn):
    """Processar região de forma otimizada"""
    
    print(f"\n Processando: {region['name']}")
    
    # Buscar dados
    results = downloader.search_data(region)
    
    if not results:
        print(f"     Nenhum dado encontrado")
        return 0
    
    print(f"     Encontrados: {len(results)} granules")
    
    # Verificar granules novos
    new_granules = []
    for granule in results[:MAX_GRANULES_PER_REGION]:  # Limitar
        granule_name = extract_granule_name(granule)
        if not check_granule_exists(granule_name, db_conn):
            new_granules.append(granule)
    
    if not new_granules:
        print(f"     Todos os granules já processados")
        return 0
    
    print(f"     Novos: {len(new_granules)} granules")
    
    processed = 0
    
    # Processar granules novos
    for granule in new_granules:
        try:
            granule_name = extract_granule_name(granule)
            print(f"     {granule_name[:30]}...")
            
            # Download e processamento
            with tempfile.TemporaryDirectory() as temp_dir:
                files = downloader.download_data([granule], temp_dir)
                
                if files:
                    df = process_netcdf_fixed(files[0], region)
                    
                    if df is not None and len(df) > 0:
                        # Pular se muito grande
                        if len(df) > MAX_PIXELS_PER_GRANULE:
                            print(f"     Pulando (muito grande: {len(df)} pixels)")
                            continue
                        
                        # Inserir no banco
                        if insert_granule_data_optimized(df, granule_name, region, db_conn):
                            print(f"     {len(df)} pixels inseridos")
                            processed += 1
                        else:
                            print(f"     Erro na inserção")
                    else:
                        print(f"     Nenhum pixel válido")
                        
        except Exception as e:
            print(f"     Erro: {str(e)[:50]}...")
            continue
    
    return processed

def process_netcdf_fixed(file_path, region):
    """
    Processamento de NetCDF
    """
    try:
        print(f"     Processando arquivo: {file_path}")
        
        # CORREÇÃO: Tentar diferentes engines em ordem de preferência
        engines_to_try = ['h5netcdf', 'netcdf4', 'scipy']
        
        ds = None
        used_engine = None
        
        for engine in engines_to_try:
            try:
                ds = xr.open_dataset(file_path, group='pixel_cloud', engine=engine)
                used_engine = engine
                print(f"     Usando engine: {engine}")
                break
            except Exception as e:
                print(f"     Engine {engine} falhou: {str(e)[:30]}...")
                continue
        
        if ds is None:
            # Tentar sem especificar engine
            try:
                ds = xr.open_dataset(file_path, group='pixel_cloud')
                used_engine = 'default'
                print(f"     Usando engine padrão")
            except Exception as e:
                print(f"     Todos os engines falharam: {e}")
                return None
        
        # Processar dados usando context manager
        with ds:
            # Verificar variáveis disponíveis
            available_vars = list(ds.variables.keys())
            print(f"     Variáveis: {len(available_vars)} encontradas")
            
            required_vars = ['latitude', 'longitude']
            if not all(var in ds.variables for var in required_vars):
                print(f"     Variáveis obrigatórias não encontradas")
                return None
            
            # Extrair dados de forma robusta
            data = {}
            
            # Coordenadas (obrigatórias)
            try:
                # Achatar arrays multidimensionais
                data['latitude'] = ds.latitude.values.flatten().astype('float32')
                data['longitude'] = ds.longitude.values.flatten().astype('float32')
                print(f"     Coordenadas extraídas: {len(data['latitude'])} pontos")
            except Exception as e:
                print(f"     Erro extraindo coordenadas: {e}")
                return None
            
            # Variáveis opcionais com tratamento individual
            optional_vars = {
                'height': 'height',
                'classification': 'classification', 
                'coherent_power': 'coherent_power'
            }
            
            for var_name, ds_var in optional_vars.items():
                try:
                    if ds_var in ds.variables:
                        var_data = ds[ds_var].values.flatten()
                        
                        # Conversão de tipo específica
                        if var_name == 'classification':
                            data[var_name] = var_data.astype('uint8')
                        else:
                            data[var_name] = var_data.astype('float32')
                        
                        print(f"     Extraída variável: {var_name}")
                except Exception as e:
                    print(f"     Falha extraindo {var_name}: {str(e)[:30]}...")
                    continue
            
            # Criar DataFrame
            try:
                df = pd.DataFrame(data)
                print(f"     DataFrame criado com {len(df)} pixels")
            except Exception as e:
                print(f"     Erro criando DataFrame: {e}")
                return None
            
            # Limpeza robusta
            original_size = len(df)
            
            # Filtrar coordenadas válidas
            df = df.dropna(subset=['latitude', 'longitude'])
            
            # Filtrar coordenadas dentro de limites razoáveis
            df = df[
                (df['latitude'] >= -90) & (df['latitude'] <= 90) &
                (df['longitude'] >= -180) & (df['longitude'] <= 180)
            ]
            
            cleaned_count = original_size - len(df)
            if cleaned_count > 0:
                print(f"     Limpeza: removidos {cleaned_count} pixels inválidos")
            
            # Filtro regional com buffer
            if region and 'bbox' in region:
                bbox = region['bbox']  # [min_lon, min_lat, max_lon, max_lat]
                
                # Buffer pequeno para compensar imprecisões
                buffer = 0.05  # ~1km
                mask = (
                    (df['longitude'] >= (bbox[0] - buffer)) & 
                    (df['longitude'] <= (bbox[2] + buffer)) &
                    (df['latitude'] >= (bbox[1] - buffer)) & 
                    (df['latitude'] <= (bbox[3] + buffer))
                )
                
                df = df[mask]
                print(f"     Filtro regional: {len(df)} pixels na região")
            
            return df
            
    except Exception as e:
        print(f"     ERRO GERAL: {e}")
        import traceback
        traceback.print_exc()
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
        print(f"     Erro inserção: {e}")
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
    
    print(f" MONITOR SWOT PRODUÇÃO- {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Timeout configurado: {MAX_EXECUTION_TIME_MINUTES} minutos")
    
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
        
        print(" Conectado ao banco de dados")
        
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
                print(f"Timeout atingido ({MAX_EXECUTION_TIME_MINUTES}min)")
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
        print(f"     {total_processed} granules processados")
        print(f"    Tempo de execução: {execution_time}")
        print(f"     {len(active_regions)} regiões verificadas")
        
        db_conn.close()
        
        return 0
        
    except Exception as e:
        print(f" ERRO CRÍTICO: {e}")
        return 1

if __name__ == "__main__":
    
    
    exit_code = main()
    print(f"\n FINALIZADO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(exit_code)