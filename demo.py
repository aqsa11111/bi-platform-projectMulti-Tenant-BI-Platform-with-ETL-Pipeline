#!/usr/bin/env python3
"""
Demo script for Multi-Tenant BI Platform
Perfect for showcasing to recruiters and on LinkedIn
"""

import sqlite3
import pandas as pd
from datetime import datetime

def showcase_project():
    """Demonstrate the BI platform capabilities"""
    
    print("🚀 MULTI-TENANT BI PLATFORM DEMO")
    print("=" * 50)
    print(f"Demo Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Connect to database
    conn = sqlite3.connect('data_warehouse.db')
    
    print("📊 PROJECT OVERVIEW:")
    print("- Multi-tenant Business Intelligence Platform")
    print("- End-to-end ETL Pipeline")
    print("- Data Warehousing with Star Schema")
    print("- Production-ready Analytics")
    print()
    
    print("🏗️ ARCHITECTURE HIGHLIGHTS:")
    print("Data Sources → ETL Pipeline → Data Warehouse → Analytics")
    print()
    
    print("📈 DATA PROCESSING METRICS:")
    
    # Campaign data
    campaigns_df = pd.read_sql_query("SELECT COUNT(*) as count FROM fact_campaigns", conn)
    print(f"✅ Marketing Campaigns: {campaigns_df['count'].iloc[0]:,} records")
    
    # Targets data
    targets_df = pd.read_sql_query("SELECT COUNT(*) as count FROM dim_sales_targets", conn)
    print(f"✅ Sales Targets: {targets_df['count'].iloc[0]:,} records")
    
    # Customers data
    customers_df = pd.read_sql_query("SELECT COUNT(*) as count FROM dim_customers", conn)
    print(f"✅ Customer Records: {customers_df['count'].iloc[0]:,} records")
    
    print()
    print("🏢 MULTI-TENANT ARCHITECTURE:")
    
    # Tenant summary
    tenant_summary = pd.read_sql_query("""
        SELECT 
            tenant_id,
            COUNT(*) as campaigns,
            ROUND(SUM(revenue), 2) as total_revenue,
            ROUND(AVG(roi), 2) as avg_roi
        FROM fact_campaigns 
        GROUP BY tenant_id
        ORDER BY tenant_id
    """, conn)
    
    for _, row in tenant_summary.iterrows():
        print(f"   {row['tenant_id']}: {row['campaigns']} campaigns, ${row['total_revenue']:,.2f} revenue, {row['avg_roi']} ROI")
    
    print()
    print("📊 SAMPLE ANALYTICS:")
    
    # Performance metrics
    metrics = pd.read_sql_query("""
        SELECT 
            ROUND(AVG(ctr), 4) as avg_ctr,
            ROUND(AVG(roi), 2) as avg_roi,
            ROUND(SUM(revenue), 2) as total_revenue,
            ROUND(SUM(spend), 2) as total_spend
        FROM fact_campaigns
    """, conn)
    
    print(f"   Average CTR: {metrics['avg_ctr'].iloc[0]:.2%}")
    print(f"   Average ROI: {metrics['avg_roi'].iloc[0]:.2f}")
    print(f"   Total Revenue: ${metrics['total_revenue'].iloc[0]:,.2f}")
    print(f"   Total Spend: ${metrics['total_spend'].iloc[0]:,.2f}")
    
    print()
    print("🎯 BUSINESS IMPACT:")
    print("   • Marketing Analytics & Campaign Tracking")
    print("   • Sales Intelligence & Target Analysis")
    print("   • Customer Analytics & Behavior Insights")
    print("   • Multi-tenant SaaS Platform Ready")
    print("   • Executive Dashboard Capabilities")
    
    print()
    print("💼 SKILLS DEMONSTRATED:")
    print("   • Data Engineering (ETL Pipeline)")
    print("   • Database Design (Star Schema)")
    print("   • Business Intelligence (Analytics)")
    print("   • Software Architecture (Multi-tenant)")
    print("   • Production Development (Error Handling)")
    print("   • Data Analysis (Pandas, SQL)")
    
    print()
    print("🛠️ TECH STACK:")
    print("   • Python 3.13 | Pandas & NumPy")
    print("   • SQLite | OpenPyXL | Flask")
    print("   • ETL Pipeline | Data Warehousing")
    print("   • Error Handling | Logging")
    
    print()
    print("🚀 READY FOR:")
    print("   • Data Engineer Positions")
    print("   • BI Developer Roles")
    print("   • Analytics Engineer Opportunities")
    print("   • Full-stack Data Positions")
    
    print()
    print("=" * 50)
    print("✅ PROJECT SUCCESSFULLY COMPLETED!")
    print("Perfect for showcasing to recruiters and on LinkedIn!")
    
    conn.close()

if __name__ == "__main__":
    showcase_project()
