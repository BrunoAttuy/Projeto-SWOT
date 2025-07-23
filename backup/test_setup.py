#!/usr/bin/env python3
"""
Teste de configuração do projeto SWOT
"""
import sys
import os
from pathlib import Path

# Adicionar src ao path
sys.path.append('src')

def test_imports():
    """Testar imports dos módulos"""
    print(" Testando imports...")
    
    try:
        from utils.config import load_config, get_regions
        print(" utils.config OK")
    except Exception as e:
        print(f" utils.config: {e}")
        return False
    
    try:
        from utils.logger import setup_logger
        print(" utils.logger OK")
    except Exception as e:
        print(f" utils.logger: {e}")
        return False
    
    try:
        from database.connection import DatabaseConnection
        print(" database.connection OK")
    except Exception as e:
        print(f" database.connection: {e}")
        return False
    
    try:
        from core.swot_downloader import SWOTDownloader
        print(" core.swot_downloader OK")
    except Exception as e:
        print(f" core.swot_downloader: {e}")
        return False
    
    return True

def test_packages():
    """Testar pacotes externos"""
    print("\n Testando pacotes...")
    
    packages = ['earthaccess', 'xarray', 'pandas', 'geopandas', 'psycopg2']
    
    for package in packages:
        try:
            __import__(package)
            print(f" {package} OK")
        except ImportError:
            print(f" {package} não instalado")
            return False
    
    return True

def test_config():
    """Testar configurações"""
    print("\n Testando configurações...")
    
    # Testar arquivo .env
    from dotenv import load_dotenv
    load_dotenv()
    
    env_vars = ['EARTHDATA_USERNAME', 'EARTHDATA_PASSWORD', 'DB_PASSWORD']
    for var in env_vars:
        value = os.getenv(var)
        if value and value != f'seu_{var.lower()}':
            print(f" {var} configurado")
        else:
            print(f" {var} precisa ser configurado no arquivo .env")
    
    # Testar regiões
    from utils.config import get_regions
    regions = get_regions()
    print(f" {len(regions)} regiões carregadas")
    
    return True

def test_database():
    """Testar conexão com banco"""
    print("\n Testando banco de dados...")
    
    try:
        from database.connection import DatabaseConnection
        db = DatabaseConnection()
        
        if db.test_connection():
            print(" Conexão com PostgreSQL OK")
            return True
        else:
            print(" Falha na conexão com PostgreSQL")
            print(" Verifique se PostgreSQL está rodando e credenciais no .env")
            return False
    except Exception as e:
        print(f" Erro testando banco: {e}")
        return False

def test_nasa():
    """Testar conexão com NASA"""
    print("\n Testando NASA Earthdata...")
    
    try:
        from core.swot_downloader import SWOTDownloader
        downloader = SWOTDownloader()
        
        if downloader.authenticate():
            print(" Autenticação NASA OK")
            return True
        else:
            print(" Falha na autenticação NASA")
            print(" Verifique credenciais EARTHDATA no .env")
            return False
    except Exception as e:
        print(f" Erro testando NASA: {e}")
        return False

def main():
    """Função principal de teste"""
    print(" Testando configuração do projeto SWOT...\n")
    
    tests = [
        ("Imports", test_imports),
        ("Pacotes", test_packages), 
        ("Configurações", test_config),
        ("Banco de Dados", test_database),
        ("NASA Earthdata", test_nasa)
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f" Erro no teste {name}: {e}")
            results.append(False)
    
    # Resumo
    print(f"\n Resumo dos testes:")
    print(f" Passou: {sum(results)}/{len(results)}")
    print(f" Falhou: {len(results) - sum(results)}/{len(results)}")
    
    if all(results):
        print("\n Todos os testes passaram! Sistema pronto para uso.")
        print("\n Próximos passos:")
        print("1. Editar config/regions.json com suas regiões")
        print("2. Executar: python monitor_daily.py")
        return True
    else:
        print("\n Alguns testes falharam. Corrija os problemas antes de continuar.")
        return False

if __name__ == "__main__":
    success = main()
    input("\nPressione Enter para fechar...")
    sys.exit(0 if success else 1)