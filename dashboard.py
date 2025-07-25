#!/usr/bin/env python3
"""
Dashboard simples do sistema SWOT
"""
import sys
sys.path.append('src')

import psycopg2
from dotenv import load_dotenv
import os

def show_status():
    load_dotenv()
    
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    
    cursor = conn.cursor()
    
    print("="*50)
    print(" STATUS SWOT MONITOR")
    print("="*50)
    
    # Totais
    cursor.execute("SELECT COUNT(*) FROM granules;")
    total_granules = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM pixel_data;")
    total_pixels = cursor.fetchone()[0]
    
    print(f" Total Granules: {total_granules:,}")
    print(f" Total Pixels: {total_pixels:,}")
    
    # Por região
    cursor.execute("""
        SELECT region_id, COUNT(*) as granules, SUM(total_pixels) as pixels
        FROM granules 
        GROUP BY region_id 
        ORDER BY granules DESC;
    """)
    
    print(f"\n Por Região:")
    for row in cursor.fetchall():
        region, granules, pixels = row
        print(f"   {region}: {granules} granules ({pixels:,} pixels)")
    
    # Últimos
    cursor.execute("""
        SELECT granule_name, total_pixels, created_at 
        FROM granules 
        ORDER BY created_at DESC 
        LIMIT 3;
    """)
    
    print(f"\n Últimos Processamentos:")
    for row in cursor.fetchall():
        name, pixels, created = row
        print(f"   {created}: {pixels:,} pixels")
    
    cursor.close()
    conn.close()
    print("="*50)

if __name__ == "__main__":
    show_status()
    input("Pressione Enter...")