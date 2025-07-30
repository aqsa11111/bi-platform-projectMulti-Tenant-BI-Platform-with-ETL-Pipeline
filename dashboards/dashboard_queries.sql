-- Campaign Performance by Tenant
-- Use this query in your BI tool with tenant_id parameter
SELECT 
    tenant_id,
    campaign_name,
    date,
    impressions,
    clicks,
    conversions,
    spend,
    revenue,
    ctr,
    roi,
    CASE 
        WHEN roi > 2.0 THEN 'Excellent'
        WHEN roi > 1.0 THEN 'Good' 
        WHEN roi > 0 THEN 'Break Even'
        ELSE 'Loss'
    END as performance_category
FROM fact_campaigns
WHERE tenant_id = 'tenant_001'  -- Replace with parameter
ORDER BY date DESC;

-- Conversion Funnel Analysis
SELECT 
    tenant_id,
    SUM(impressions) as "1_Impressions",
    SUM(clicks) as "2_Clicks", 
    SUM(conversions) as "3_Conversions",
    ROUND((SUM(clicks) * 100.0 / SUM(impressions)), 2) as "CTR_Percent",
    ROUND((SUM(conversions) * 100.0 / SUM(clicks)), 2) as "Conversion_Rate_Percent"
FROM fact_campaigns 
WHERE tenant_id = 'tenant_001'  -- Replace with parameter
GROUP BY tenant_id;

-- Time Series Performance (Last 30 days)
SELECT 
    date,
    tenant_id,
    SUM(conversions) as daily_conversions,
    SUM(revenue) as daily_revenue,
    SUM(spend) as daily_spend,
    SUM(revenue) - SUM(spend) as daily_profit
FROM fact_campaigns 
WHERE tenant_id = 'tenant_001'  -- Replace with parameter
  AND date >= date('now', '-30 days')
GROUP BY date, tenant_id
ORDER BY date;
