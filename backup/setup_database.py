#!/usr/bin/env python3
"""
Configurar estrutura do banco de dados SWOT
"""
import sys
import os
sys.path.append('src')

from database.connection import DatabaseConnection
import psycopg2

def create_tables():
    """Criar tabelas necessárias"""
    
    # SQL para criar tabelas
    sql_commands = [
        # Tabela de missões
        """
        CREATE TABLE IF NOT EXISTS missions (
            mission_id SERIAL PRIMARY KEY,
            mission_name VARCHAR(100) NOT NULL,
            launch_date DATE,
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        
        # Inserir missão SWOT
        """
        INSERT INTO missions (mission_name, launch_date, status)
        VALUES ('SWOT', '2022-12-16', 'operational')
        ON CONFLICT DO NOTHING;
        """,
        
        # Tabela de classificações
        """
        CREATE TABLE IF NOT EXISTS classification_types (
            class_id INTEGER PRIMARY KEY,
            class_name VARCHAR(100) NOT NULL,
            class_description TEXT
        );
        """,
        
        # Inserir tipos de classificação
        """
        INSERT INTO classification_types VALUES 
        (0, 'Land', 'Area terrestre'),
        (1, 'Land near water', 'Terra proxima a agua'),
        (2, 'Water near land', 'Agua proxima a terra'),
        (3, 'Open water', 'Agua aberta'),
        (4, 'Dark water', 'Agua escura'),
        (5, 'Low coherence water near land', 'Agua de baixa coerencia proxima a terra'),
        (6, 'Open low coherence water', 'Agua aberta de baixa coerencia')
        ON CONFLICT (class_id) DO NOTHING;
        """,
        
        # Tabela de granules
        """
        CREATE TABLE IF NOT EXISTS granules (
            granule_id SERIAL PRIMARY KEY,
            granule_name VARCHAR(200) UNIQUE NOT NULL,
            mission_id INTEGER REFERENCES missions(mission_id),
            acquisition_start TIMESTAMP,
            acquisition_end TIMESTAMP,
            region_id VARCHAR(50),
            file_size_mb DECIMAL(10,2),
            bbox_min_lat DECIMAL(10,6),
            bbox_max_lat DECIMAL(10,6),
            bbox_min_lon DECIMAL(11,6),
            bbox_max_lon DECIMAL(11,6),
            total_pixels INTEGER,
            processing_status VARCHAR(20) DEFAULT 'completed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        
        # Tabela de dados dos pixels
        """
        CREATE TABLE IF NOT EXISTS pixel_data (
            pixel_id BIGSERIAL PRIMARY KEY,
            granule_id INTEGER NOT NULL REFERENCES granules(granule_id) ON DELETE CASCADE,
            latitude DECIMAL(10,6) NOT NULL,
            longitude DECIMAL(11,6) NOT NULL,
            height_m DECIMAL(8,3),
            classification_id INTEGER REFERENCES classification_types(class_id),
            coherent_power DECIMAL(15,6),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        
        # Índices para performance
        """
        CREATE INDEX IF NOT EXISTS idx_granules_name ON granules(granule_name);
        CREATE INDEX IF NOT EXISTS idx_granules_region ON granules(region_id);
        CREATE INDEX IF NOT EXISTS idx_pixel_granule ON pixel_data(granule_id);
        CREATE INDEX IF NOT EXISTS idx_pixel_coords ON pixel_data(latitude, longitude);
        CREATE INDEX IF NOT EXISTS idx_pixel_classification ON pixel_data(classification_id);
        """
    ]
    
    try:
        # Conectar ao banco
        from dotenv import load_dotenv
        load_dotenv()
        
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        
        cursor = conn.cursor()
        
        print(" Criando estrutura do banco de dados...")
        
        for i, sql in enumerate(sql_commands):
            try:
                cursor.execute(sql)
                print(f" Comando {i+1}/{len(sql_commands)} executado")
            except Exception as e:
                print(f" Comando {i+1}: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(" Estrutura do banco criada com sucesso!")
        return True
        
    except Exception as e:
        print(f" Erro: {e}")
        return False

if __name__ == "__main__":
    print(" Configurando banco de dados SWOT...")
    
    if create_tables():
        print("\n Banco configurado! Próximo passo: criar monitor diário")
    else:
        print("\n Erro na configuração do banco")
    
    input("Pressione Enter para continuar...")