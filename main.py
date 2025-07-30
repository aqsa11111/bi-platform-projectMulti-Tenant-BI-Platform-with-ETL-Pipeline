#!/usr/bin/env python3
"""
Multi-Tenant BI Platform - ETL Pipeline
A comprehensive data pipeline for marketing campaign analytics
"""

import pandas as pd
import numpy as np
import sqlite3
import logging
import requests
from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional
import openpyxl
from config import *

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class DataGenerator:
    """Generates sample data for the BI platform"""
    
    def __init__(self):
        self.tenants = TENANTS
        self.regions = REGIONS
        self.campaign_types = ['Social Media', 'Search', 'Display', 'Video', 'Email']
        self.products = ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
        
    def generate_marketing_campaigns(self) -> pd.DataFrame:
        """Generate marketing campaigns data with multi-tenant structure"""
        logger.info("=== Generating Sample Data ===")
        
        data = []
        start_date = datetime(2024, 10, 1)
        
        for tenant in self.tenants:
            for i in range(100):  # 100 campaigns per tenant
                campaign_date = start_date + timedelta(days=random.randint(0, 60))
                
                # Generate realistic campaign metrics
                impressions = random.randint(5000, 50000)
                clicks = random.randint(100, 5000)
                conversions = random.randint(10, 500)
                spend = random.uniform(100, 10000)
                revenue = spend * random.uniform(0.5, 3.0)  # ROI between 0.5x and 3x
                
                campaign_data = {
                    'tenant_id': tenant,
                    'campaign_id': f'camp_{tenant}_{i:03d}',
                    'campaign_name': f'Campaign {i+1} - {random.choice(self.campaign_types)}',
                    'date': campaign_date.strftime('%Y-%m-%d'),
                    'impressions': impressions,
                    'clicks': clicks,
                    'conversions': conversions,
                    'spend': round(spend, 2),
                    'revenue': round(revenue, 2),
                    'region': random.choice(self.regions),
                    'product': random.choice(self.products)
                }
                data.append(campaign_data)
        
        df = pd.DataFrame(data)
        df.to_csv(CSV_FILE, index=False)
        logger.info(f"Generated {CSV_FILE} with {len(df)} records")
        return df
    
    def generate_sales_targets(self) -> pd.DataFrame:
        """Generate sales targets data"""
        data = []
        
        for tenant in self.tenants:
            for region in self.regions:
                for product in self.products:
                    for month in range(1, 13):
                        target_data = {
                            'tenant_id': tenant,
                            'region': region,
                            'product': product,
                            'month': month,
                            'year': 2024,
                            'target_revenue': random.uniform(5000, 50000),
                            'target_conversions': random.randint(50, 500),
                            'target_spend': random.uniform(2000, 20000)
                        }
                        data.append(target_data)
        
        df = pd.DataFrame(data)
        df.to_excel(EXCEL_FILE, index=False)
        logger.info(f"Generated {EXCEL_FILE} with {len(df)} records")
        return df

class ETLPipeline:
    """Main ETL pipeline for the BI platform"""
    
    def __init__(self):
        self.db_path = DATABASE_PATH
        self.conn = None
        self.setup_database()
    
    def setup_database(self):
        """Initialize database and create tables"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()
            
            # Create fact table for campaigns
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fact_campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    campaign_name TEXT,
                    date TEXT,
                    impressions INTEGER,
                    clicks INTEGER,
                    conversions INTEGER,
                    spend REAL,
                    revenue REAL,
                    ctr REAL,
                    roi REAL,
                    region TEXT,
                    product TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create dimension table for sales targets
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS dim_sales_targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    region TEXT,
                    product TEXT,
                    month INTEGER,
                    year INTEGER,
                    target_revenue REAL,
                    target_conversions INTEGER,
                    target_spend REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create dimension table for customers (mock API data)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS dim_customers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    customer_id TEXT NOT NULL,
                    customer_name TEXT,
                    region TEXT,
                    product_preference TEXT,
                    total_spent REAL,
                    total_orders INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            self.conn.commit()
            logger.info("Database tables created successfully")
            
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise
    
    def extract_csv_data(self, file_path: str) -> pd.DataFrame:
        """Extract data from CSV file"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Extracted {len(df)} records from {file_path}")
            return df
        except Exception as e:
            logger.error(f"CSV extraction failed: {e}")
            raise
    
    def extract_excel_data(self, file_path: str) -> pd.DataFrame:
        """Extract data from Excel file"""
        try:
            df = pd.read_excel(file_path)
            logger.info(f"Extracted {len(df)} records from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Excel extraction failed: {e}")
            raise
    
    def extract_api_data(self, endpoint: str) -> pd.DataFrame:
        """Extract data from API (mock implementation)"""
        try:
            # Mock API data for customers
            data = []
            for tenant in TENANTS:
                for i in range(50):  # 50 customers per tenant
                    customer_data = {
                        'tenant_id': tenant,
                        'customer_id': f'cust_{tenant}_{i:03d}',
                        'customer_name': f'Customer {i+1}',
                        'region': random.choice(REGIONS),
                        'product_preference': random.choice(['Product A', 'Product B', 'Product C']),
                        'total_spent': random.uniform(100, 5000),
                        'total_orders': random.randint(1, 20)
                    }
                    data.append(customer_data)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} records from API (mock)")
            return df
            
        except Exception as e:
            logger.error(f"API extraction failed: {e}")
            # Fallback to mock data
            logger.info("Using fallback mock data")
            return self._generate_mock_customer_data()
    
    def _generate_mock_customer_data(self) -> pd.DataFrame:
        """Generate mock customer data as fallback"""
        data = []
        for tenant in TENANTS:
            for i in range(50):
                customer_data = {
                    'tenant_id': tenant,
                    'customer_id': f'cust_{tenant}_{i:03d}',
                    'customer_name': f'Customer {i+1}',
                    'region': random.choice(REGIONS),
                    'product_preference': random.choice(['Product A', 'Product B', 'Product C']),
                    'total_spent': random.uniform(100, 5000),
                    'total_orders': random.randint(1, 20)
                }
                data.append(customer_data)
        
        df = pd.DataFrame(data)
        logger.info(f"Generated mock customer data: {len(df)} records")
        return df
    
    def transform_campaigns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform campaigns data with calculated metrics"""
        try:
            # Calculate CTR (Click-Through Rate)
            df['ctr'] = (df['clicks'] / df['impressions']).round(4)
            
            # Calculate ROI (Return on Investment)
            df['roi'] = ((df['revenue'] - df['spend']) / df['spend']).round(2)
            
            # Handle division by zero
            df['ctr'] = df['ctr'].fillna(0)
            df['roi'] = df['roi'].fillna(0)
            
            logger.info(f"Transformed campaigns data: {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Campaign transformation failed: {e}")
            raise
    
    def transform_targets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform sales targets data"""
        try:
            # Add calculated fields if needed
            df['target_roi'] = (df['target_revenue'] / df['target_spend']).round(2)
            df['target_roi'] = df['target_roi'].fillna(0)
            
            logger.info(f"Transformed targets data: {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Targets transformation failed: {e}")
            raise
    
    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform customers data"""
        try:
            # Add calculated fields if needed
            df['avg_order_value'] = (df['total_spent'] / df['total_orders']).round(2)
            df['avg_order_value'] = df['avg_order_value'].fillna(0)
            
            logger.info(f"Transformed customers data: {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Customers transformation failed: {e}")
            raise
    
    def load_to_database(self, df: pd.DataFrame, table_name: str):
        """Load transformed data to database"""
        try:
            df.to_sql(table_name, self.conn, if_exists='append', index=False)
            logger.info(f"Loaded {len(df)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Database loading failed for {table_name}: {e}")
            raise
    
    def execute_dashboard_query(self, query_type: str) -> pd.DataFrame:
        """Execute dashboard queries"""
        try:
            if query_type == 'campaign_summary':
                query = '''
                    SELECT 
                        tenant_id,
                        COUNT(*) as total_campaigns,
                        SUM(impressions) as total_impressions,
                        SUM(clicks) as total_clicks,
                        SUM(conversions) as total_conversions,
                        SUM(spend) as total_spend,
                        SUM(revenue) as total_revenue,
                        ROUND(AVG(ctr), 4) as avg_ctr,
                        ROUND(AVG(roi), 2) as avg_roi
                    FROM fact_campaigns
                    GROUP BY tenant_id
                    ORDER BY tenant_id
                '''
            elif query_type == 'daily_performance':
                query = '''
                    SELECT 
                        tenant_id,
                        date,
                        SUM(conversions) as daily_conversions,
                        SUM(revenue) as daily_revenue,
                        SUM(spend) as daily_spend
                    FROM fact_campaigns
                    GROUP BY tenant_id, date
                    ORDER BY tenant_id, date
                    LIMIT 50
                '''
            else:
                raise ValueError(f"Unknown query type: {query_type}")
            
            df = pd.read_sql_query(query, self.conn)
            return df
            
        except Exception as e:
            logger.error(f"Dashboard query failed: {e}")
            raise
    
    def close_connection(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """Main execution function"""
    logger.info("=== Running ETL Pipeline ===")
    
    try:
        # Initialize components
        generator = DataGenerator()
        etl = ETLPipeline()
        
        logger.info("=== DAY 1: Data Ingestion & Transformation ===")
        
        # Generate sample data
        logger.info("=== Generating Sample Data ===")
        campaigns_df = generator.generate_marketing_campaigns()
        targets_df = generator.generate_sales_targets()
        
        # Extract data
        campaigns_data = etl.extract_csv_data(CSV_FILE)
        targets_data = etl.extract_excel_data(EXCEL_FILE)
        customers_data = etl.extract_api_data("customers")
        
        # Transform data
        campaigns_transformed = etl.transform_campaigns(campaigns_data)
        targets_transformed = etl.transform_targets(targets_data)
        customers_transformed = etl.transform_customers(customers_data)
        
        logger.info("=== DAY 2: Data Warehouse Loading ===")
        
        # Load to database
        etl.load_to_database(campaigns_transformed, 'fact_campaigns')
        etl.load_to_database(targets_transformed, 'dim_sales_targets')
        etl.load_to_database(customers_transformed, 'dim_customers')
        
        logger.info("ETL Pipeline completed successfully!")
        
        # Generate dashboard summaries
        print("\n" + "="*50)
        print("--- Campaign Summary by Tenant ---")
        summary_df = etl.execute_dashboard_query('campaign_summary')
        print(summary_df.to_string(index=False))
        
        print("\n" + "="*50)
        print("--- Daily Performance (First 10 records) ---")
        daily_df = etl.execute_dashboard_query('daily_performance')
        print(daily_df.head(10).to_string(index=False))
        
        etl.close_connection()
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
