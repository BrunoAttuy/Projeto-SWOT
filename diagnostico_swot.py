#!/usr/bin/env python3
"""
Diagnóstico completo da busca SWOT
"""
import sys
sys.path.append('src')

import earthaccess
from datetime import datetime, timedelta
from utils.config import get_regions

def diagnostico_completo():
    """Diagnóstico detalhado"""
    
    print("🔍 DIAGNÓSTICO SWOT - Investigando problemas...")
    
    # 1. Testar autenticação
    print("\n1.  Testando autenticação...")
    try:
        auth = earthaccess.login()
        if auth:
            print("    Autenticação OK")
        else:
            print("    Falha na autenticação")
            return
    except Exception as e:
        print(f"    Erro na autenticação: {e}")
        return
    
    # 2. Verificar regiões configuradas
    print("\n2.  Verificando regiões...")
    regions = get_regions()
    
    if not regions:
        print("    Nenhuma região configurada!")
        return
    
    for region in regions:
        print(f"    {region['name']}: {region['bbox']}")
    
    # 3. Testar busca sem filtro de região (global)
    print("\n3.  Testando busca global (sem região)...")
    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)  # Só 7 dias para teste global
        
        print(f"   Período: {start_date} até {end_date}")
        
        results_global = earthaccess.search_data(
            short_name='SWOT_L2_HR_PIXC',
            temporal=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        
        print(f"    Dados SWOT globais encontrados: {len(results_global)}")
        
        if results_global:
            print("    Primeiros granules globais:")
            for i, granule in enumerate(results_global[:3]):
                print(f"      {i+1}. {str(granule)[:80]}...")
        
    except Exception as e:
        print(f"    Erro na busca global: {e}")
        return
    
    # 4. Testar diferentes produtos SWOT
    print("\n4.  Testando diferentes produtos SWOT...")
    produtos = [
        'SWOT_L2_HR_PIXC',
        'SWOT_L2_LR_SSH',
        'SWOT_L2_HR_RASTER'
    ]
    
    for produto in produtos:
        try:
            results = earthaccess.search_data(
                short_name=produto,
                temporal=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            )
            print(f"    {produto}: {len(results)} granules")
        except Exception as e:
            print(f"    {produto}: Erro - {e}")
    
    # 5. Testar região conhecida com dados (EUA)
    print("\n5. 🇺🇸 Testando região dos EUA (alta probabilidade)...")
    try:
        # Região dos Grandes Lagos (EUA) - alta probabilidade de dados
        bbox_eua = [41.0, -90.0, 49.0, -76.0]  # (min_lat, min_lon, max_lat, max_lon)
        
        results_eua = earthaccess.search_data(
            short_name='SWOT_L2_HR_PIXC',
            bounding_box=bbox_eua,
            temporal=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        
        print(f"    Região EUA: {len(results_eua)} granules")
        
    except Exception as e:
        print(f"    Erro região EUA: {e}")
    
    # 6. Verificar status da missão SWOT
    print("\n6.  Verificando disponibilidade de dados...")
    try:
        # Buscar dados muito recentes (último dia)
        ontem = (datetime.now() - timedelta(days=1)).date()
        hoje = datetime.now().date()
        
        results_recentes = earthaccess.search_data(
            short_name='SWOT_L2_HR_PIXC',
            temporal=(ontem.strftime('%Y-%m-%d'), hoje.strftime('%Y-%m-%d'))
        )
        
        print(f"    Dados das últimas 24h: {len(results_recentes)}")
        
        # Buscar dados mais antigos para comparar
        antiga_start = datetime(2023, 12, 1).date()
        antiga_end = datetime(2023, 12, 31).date()
        
        results_antigos = earthaccess.search_data(
            short_name='SWOT_L2_HR_PIXC',
            temporal=(antiga_start.strftime('%Y-%m-%d'), antiga_end.strftime('%Y-%m-%d'))
        )
        
        print(f"    Dados de dezembro 2023: {len(results_antigos)}")
        
    except Exception as e:
        print(f"    Erro verificando disponibilidade: {e}")
    
    # 7. Resumo e recomendações
    print("\n7.  RESUMO E RECOMENDAÇÕES:")
    
    if len(results_global) == 0:
        print("     PROBLEMA: Nenhum dado SWOT encontrado globalmente")
        print("    Possíveis causas:")
        print("      - Dados SWOT ainda não disponibilizados publicamente")
        print("      - Problema na URL/endpoint da NASA")
        print("      - Missão SWOT em fase de calibração")
        print("    Soluções:")
        print("      - Verificar status da missão em: https://swot.jpl.nasa.gov/")
        print("      - Testar earthaccess com outros produtos")
        print("      - Aguardar liberação de mais dados")
    else:
        print("    Dados SWOT existem, problema pode ser:")
        print("      - Coordenadas da região incorretas")
        print("      - Região sem cobertura SWOT no período")
        print("      - Formato do bbox incorreto")

if __name__ == "__main__":
    diagnostico_completo()
    input("\nPressione Enter para fechar...")