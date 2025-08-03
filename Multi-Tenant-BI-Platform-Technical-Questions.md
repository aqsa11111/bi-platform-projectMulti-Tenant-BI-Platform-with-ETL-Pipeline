# Multi-Tenant BI Platform - Technical Interview Questions & Answers

## **Database & SQL Questions**

### Q1: How would you implement efficient data partitioning for a multi-tenant data warehouse?

**Answer:**
```sql
-- Horizontal partitioning by tenant and date
CREATE PARTITION FUNCTION pf_TenantDatePartition (NVARCHAR(50), DATE)
AS RANGE RIGHT FOR VALUES 
('TENANT001', '2023-01-01'),
('TENANT001', '2023-02-01'),
('TENANT001', '2023-03-01'),
('TENANT002', '2023-01-01'),
('TENANT002', '2023-02-01'),
('TENANT003', '2023-01-01');

CREATE PARTITION SCHEME ps_TenantDatePartition
AS PARTITION pf_TenantDatePartition
ALL TO ([PRIMARY]);

-- Create partitioned fact table
CREATE TABLE fact_sales (
    sale_id BIGINT IDENTITY(1,1),
    tenant_id NVARCHAR(50) NOT NULL,
    sale_date DATE NOT NULL,
    customer_key INT,
    product_key INT,
    sales_amount DECIMAL(18,2),
    quantity INT,
    INDEX IX_TenantDate CLUSTERED (tenant_id, sale_date)
) ON ps_TenantDatePartition(tenant_id, sale_date);

-- Benefits:
-- 1. Partition elimination improves query performance
-- 2. Parallel processing across partitions
-- 3. Easier maintenance (backup, archiving)
-- 4. Better resource allocation per tenant
```

### Q2: Write a stored procedure to handle SCD Type 2 with audit trails for multi-tenant scenario.

**Answer:**
```sql
CREATE PROCEDURE usp_UpdateCustomerDimensionSCD2
    @TenantId NVARCHAR(50),
    @CustomerId NVARCHAR(50),
    @CustomerName NVARCHAR(255),
    @CustomerCategory NVARCHAR(50),
    @CustomerEmail NVARCHAR(255),
    @ModifiedBy NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ExistingRecord TABLE (
        customer_key INT,
        customer_name NVARCHAR(255),
        customer_category NVARCHAR(50),
        customer_email NVARCHAR(255)
    );
    
    -- Get current active record
    INSERT INTO @ExistingRecord
    SELECT customer_key, customer_name, customer_category, customer_email
    FROM dim_customer 
    WHERE tenant_id = @TenantId 
        AND customer_id = @CustomerId 
        AND is_current = 1;
    
    -- Check if any changes exist
    IF EXISTS (
        SELECT 1 FROM @ExistingRecord e
        WHERE e.customer_name != @CustomerName 
           OR e.customer_category != @CustomerCategory
           OR e.customer_email != @CustomerEmail
    )
    BEGIN
        DECLARE @CustomerKey INT;
        SELECT @CustomerKey = customer_key FROM @ExistingRecord;
        
        -- Insert audit record
        INSERT INTO dim_customer_audit (
            customer_key, tenant_id, customer_id, 
            old_name, new_name, old_category, new_category,
            old_email, new_email, modified_by, modified_date
        )
        SELECT 
            @CustomerKey, @TenantId, @CustomerId,
            e.customer_name, @CustomerName,
            e.customer_category, @CustomerCategory,
            e.customer_email, @CustomerEmail,
            @ModifiedBy, GETUTCDATE()
        FROM @ExistingRecord e;
        
        -- Close current record
        UPDATE dim_customer 
        SET valid_to = GETUTCDATE(),
            is_current = 0,
            modified_by = @ModifiedBy,
            modified_date = GETUTCDATE()
        WHERE tenant_id = @TenantId 
            AND customer_id = @CustomerId 
            AND is_current = 1;
        
        -- Insert new record
        INSERT INTO dim_customer (
            tenant_id, customer_id, customer_name, customer_category,
            customer_email, valid_from, valid_to, is_current,
            created_by, created_date
        )
        VALUES (
            @TenantId, @CustomerId, @CustomerName, @CustomerCategory,
            @CustomerEmail, GETUTCDATE(), '9999-12-31', 1,
            @ModifiedBy, GETUTCDATE()
        );
    END
END
```

### Q3: How would you implement row-level security for multi-tenant access?

**Answer:**
```sql
-- 1. Create security predicate function
CREATE FUNCTION dbo.fn_TenantSecurityPredicate(@TenantId NVARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN 
    SELECT 1 AS fn_securitypredicate_result 
    WHERE @TenantId = CAST(SESSION_CONTEXT(N'TenantId') AS NVARCHAR(50))
       OR IS_MEMBER('db_owner') = 1;

-- 2. Create security policy
CREATE SECURITY POLICY TenantSecurityPolicy
ADD FILTER PREDICATE dbo.fn_TenantSecurityPredicate(tenant_id) ON dbo.fact_sales,
ADD FILTER PREDICATE dbo.fn_TenantSecurityPredicate(tenant_id) ON dbo.dim_customer,
ADD FILTER PREDICATE dbo.fn_TenantSecurityPredicate(tenant_id) ON dbo.dim_product
WITH (STATE = ON);

-- 3. Set session context in application
-- EXEC sp_set_session_context @key = N'TenantId', @value = 'TENANT001';

-- 4. Alternative approach using user mapping
CREATE FUNCTION dbo.fn_UserTenantSecurityPredicate(@TenantId NVARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN 
    SELECT 1 AS fn_securitypredicate_result 
    WHERE @TenantId IN (
        SELECT tenant_id 
        FROM dbo.user_tenant_mapping 
        WHERE username = USER_NAME()
    ) OR IS_MEMBER('db_owner') = 1;
```

---

## **ETL & Data Integration Questions**

### Q4: Design an incremental loading strategy for multi-tenant ETL pipeline.

**Answer:**
```sql
-- 1. Create control table for tracking last load times
CREATE TABLE etl_control_log (
    tenant_id NVARCHAR(50),
    table_name NVARCHAR(128),
    last_load_date DATETIME2,
    last_load_status NVARCHAR(20),
    records_processed INT,
    load_duration_seconds INT,
    created_date DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (tenant_id, table_name)
);

-- 2. Incremental loading procedure
CREATE PROCEDURE usp_IncrementalLoadSales
    @TenantId NVARCHAR(50)
AS
BEGIN
    DECLARE @LastLoadDate DATETIME2;
    DECLARE @CurrentLoadDate DATETIME2 = GETUTCDATE();
    DECLARE @RecordsProcessed INT = 0;
    DECLARE @StartTime DATETIME2 = GETUTCDATE();
    
    -- Get last successful load date
    SELECT @LastLoadDate = ISNULL(last_load_date, '1900-01-01')
    FROM etl_control_log 
    WHERE tenant_id = @TenantId AND table_name = 'fact_sales';
    
    BEGIN TRY
        -- Extract changed records using CDC or timestamp
        WITH ChangeData AS (
            SELECT *
            FROM source_sales_table 
            WHERE tenant_id = @TenantId 
                AND (modified_date > @LastLoadDate 
                     OR created_date > @LastLoadDate)
        )
        -- Upsert logic
        MERGE fact_sales AS target
        USING ChangeData AS source
        ON target.tenant_id = source.tenant_id 
           AND target.source_sale_id = source.sale_id
        
        WHEN MATCHED AND source.modified_date > target.last_updated THEN
            UPDATE SET 
                customer_key = source.customer_key,
                product_key = source.product_key,
                sales_amount = source.sales_amount,
                quantity = source.quantity,
                last_updated = @CurrentLoadDate
        
        WHEN NOT MATCHED THEN
            INSERT (tenant_id, source_sale_id, customer_key, product_key, 
                   sales_amount, quantity, created_date, last_updated)
            VALUES (source.tenant_id, source.sale_id, source.customer_key,
                   source.product_key, source.sales_amount, source.quantity,
                   @CurrentLoadDate, @CurrentLoadDate);
        
        SET @RecordsProcessed = @@ROWCOUNT;
        
        -- Update control log
        MERGE etl_control_log AS target
        USING (SELECT @TenantId AS tenant_id, 'fact_sales' AS table_name) AS source
        ON target.tenant_id = source.tenant_id 
           AND target.table_name = source.table_name
        WHEN MATCHED THEN
            UPDATE SET 
                last_load_date = @CurrentLoadDate,
                last_load_status = 'SUCCESS',
                records_processed = @RecordsProcessed,
                load_duration_seconds = DATEDIFF(SECOND, @StartTime, GETUTCDATE())
        WHEN NOT MATCHED THEN
            INSERT (tenant_id, table_name, last_load_date, last_load_status, 
                   records_processed, load_duration_seconds)
            VALUES (@TenantId, 'fact_sales', @CurrentLoadDate, 'SUCCESS',
                   @RecordsProcessed, DATEDIFF(SECOND, @StartTime, GETUTCDATE()));
                   
    END TRY
    BEGIN CATCH
        -- Log error
        UPDATE etl_control_log 
        SET last_load_status = 'FAILED',
            load_duration_seconds = DATEDIFF(SECOND, @StartTime, GETUTCDATE())
        WHERE tenant_id = @TenantId AND table_name = 'fact_sales';
        
        THROW;
    END CATCH
END
```

### Q5: How would you implement data quality checks in your ETL pipeline?

**Answer:**
```python
# Data quality framework in Python/PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class DataQualityChecker:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.quality_results = []
    
    def validate_schema(self, df, expected_schema):
        """Validate DataFrame schema against expected schema"""
        actual_fields = set((f.name, f.dataType) for f in df.schema.fields)
        expected_fields = set((f.name, f.dataType) for f in expected_schema.fields)
        
        missing_fields = expected_fields - actual_fields
        extra_fields = actual_fields - expected_fields
        
        if missing_fields or extra_fields:
            raise ValueError(f"Schema mismatch - Missing: {missing_fields}, Extra: {extra_fields}")
        
        return True
    
    def check_null_values(self, df, tenant_id, critical_columns):
        """Check for null values in critical columns"""
        null_counts = {}
        total_records = df.count()
        
        for column in critical_columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
            
            null_counts[column] = {
                'null_count': null_count,
                'null_percentage': null_percentage
            }
            
            # Fail if critical columns have > 5% nulls
            if null_percentage > 5:
                self.log_quality_issue(tenant_id, f"High null percentage in {column}: {null_percentage}%")
        
        return null_counts
    
    def check_data_freshness(self, df, tenant_id, date_column, max_age_hours=24):
        """Check if data is fresh enough"""
        max_date = df.agg(max(col(date_column))).collect()[0][0]
        current_time = datetime.now()
        
        if max_date:
            age_hours = (current_time - max_date).total_seconds() / 3600
            if age_hours > max_age_hours:
                self.log_quality_issue(tenant_id, f"Data is stale: {age_hours} hours old")
                return False
        
        return True
    
    def check_business_rules(self, df, tenant_id):
        """Validate business-specific rules"""
        quality_checks = {
            'negative_amounts': df.filter(col('sales_amount') < 0).count(),
            'future_dates': df.filter(col('sale_date') > current_date()).count(),
            'invalid_quantities': df.filter(col('quantity') <= 0).count(),
            'duplicate_records': df.count() - df.dropDuplicates(['tenant_id', 'sale_id']).count()
        }
        
        failed_checks = []
        for check_name, failure_count in quality_checks.items():
            if failure_count > 0:
                failed_checks.append(f"{check_name}: {failure_count} records")
                self.log_quality_issue(tenant_id, f"{check_name}: {failure_count} invalid records")
        
        return len(failed_checks) == 0, failed_checks
    
    def check_referential_integrity(self, fact_df, dim_dfs, tenant_id):
        """Check foreign key relationships"""
        integrity_issues = []
        
        # Check customer references
        if 'dim_customer' in dim_dfs:
            orphaned_customers = fact_df.join(
                dim_dfs['dim_customer'], 
                (fact_df.customer_key == dim_dfs['dim_customer'].customer_key) &
                (fact_df.tenant_id == dim_dfs['dim_customer'].tenant_id),
                'left_anti'
            ).filter(col('customer_key').isNotNull()).count()
            
            if orphaned_customers > 0:
                integrity_issues.append(f"Orphaned customer references: {orphaned_customers}")
        
        return len(integrity_issues) == 0, integrity_issues
    
    def log_quality_issue(self, tenant_id, issue_description):
        """Log quality issues to database"""
        quality_record = {
            'tenant_id': tenant_id,
            'check_date': datetime.now(),
            'issue_type': 'DATA_QUALITY',
            'description': issue_description,
            'severity': 'HIGH'
        }
        self.quality_results.append(quality_record)
        logging.error(f"Data Quality Issue for {tenant_id}: {issue_description}")

# Usage in ETL pipeline
def run_quality_checks(tenant_id, source_df):
    quality_checker = DataQualityChecker(spark)
    
    # Define critical columns per tenant
    critical_columns = ['customer_id', 'product_id', 'sale_date', 'sales_amount']
    
    try:
        # Run all quality checks
        quality_checker.check_null_values(source_df, tenant_id, critical_columns)
        quality_checker.check_data_freshness(source_df, tenant_id, 'sale_date')
        is_valid, failed_checks = quality_checker.check_business_rules(source_df, tenant_id)
        
        if not is_valid:
            # Send alert or stop processing
            send_quality_alert(tenant_id, failed_checks)
            return False
            
        return True
        
    except Exception as e:
        logging.error(f"Quality check failed for {tenant_id}: {str(e)}")
        return False
```

---

## **Power BI & DAX Questions**

### Q6: Write advanced DAX measures for multi-tenant scenarios with dynamic calculations.

**Answer:**
```dax
-- 1. Dynamic measure based on tenant configuration
Revenue Calculation = 
VAR CurrentTenant = SELECTEDVALUE(Sales[TenantId])
VAR RevenueMethod = 
    LOOKUPVALUE(
        TenantConfig[RevenueCalculationMethod],
        TenantConfig[TenantId], CurrentTenant
    )
VAR GrossRevenue = SUM(Sales[GrossAmount])
VAR NetRevenue = SUM(Sales[NetAmount])
VAR AdjustedRevenue = SUM(Sales[AdjustedAmount])

RETURN
SWITCH(
    RevenueMethod,
    "GROSS", GrossRevenue,
    "NET", NetRevenue,
    "ADJUSTED", AdjustedRevenue,
    GrossRevenue  -- Default
)

-- 2. Tenant-specific target comparison
Revenue vs Target % = 
VAR CurrentTenant = SELECTEDVALUE(Sales[TenantId])
VAR CurrentPeriod = MAX(Sales[Date])
VAR ActualRevenue = [Revenue Calculation]
VAR TargetRevenue = 
    CALCULATE(
        SUM(Targets[TargetAmount]),
        Targets[TenantId] = CurrentTenant,
        Targets[TargetDate] = CurrentPeriod
    )

RETURN
DIVIDE(ActualRevenue, TargetRevenue, 0) - 1

-- 3. Dynamic time intelligence based on tenant fiscal year
YTD Revenue = 
VAR CurrentTenant = SELECTEDVALUE(Sales[TenantId])
VAR FiscalYearStart = 
    LOOKUPVALUE(
        TenantConfig[FiscalYearStartMonth],
        TenantConfig[TenantId], CurrentTenant
    )
VAR CustomYTD = 
    CALCULATE(
        [Revenue Calculation],
        DATESYTD(
            Calendar[Date],
            DATE(YEAR(TODAY()), FiscalYearStart, 1)
        )
    )

RETURN CustomYTD

-- 4. Multi-tenant ranking with security context
Product Rank by Tenant = 
VAR CurrentTenant = SELECTEDVALUE(Sales[TenantId])
VAR CurrentProduct = SELECTEDVALUE(Products[ProductName])
VAR ProductRevenue = [Revenue Calculation]

RETURN
RANKX(
    FILTER(
        ALL(Products[ProductName]),
        CALCULATE([Revenue Calculation]) > 0
    ),
    CALCULATE([Revenue Calculation]),
    ,DESC
)

-- 5. Cross-tenant benchmarking (for admin users only)
Tenant Performance Percentile = 
VAR CurrentTenant = SELECTEDVALUE(Sales[TenantId])
VAR CurrentTenantRevenue = [Revenue Calculation]
VAR AllTenantRevenues = 
    ADDCOLUMNS(
        VALUES(Sales[TenantId]),
        "TenantRevenue", [Revenue Calculation]
    )

RETURN
IF(
    USERNAME() = "admin@company.com",
    PERCENTRANK.INC(AllTenantRevenues, CurrentTenantRevenue),
    BLANK()
)

-- 6. Dynamic drilling with security
Drill Through Measure = 
VAR UserTenant = 
    LOOKUPVALUE(
        UserTenantMapping[TenantId],
        UserTenantMapping[Username], USERNAME()
    )
VAR SelectedTenant = SELECTEDVALUE(Sales[TenantId])

RETURN
IF(
    UserTenant = SelectedTenant OR USERNAME() = "admin@company.com",
    [Revenue Calculation],
    BLANK()
)
```

### Q7: How would you implement dynamic RLS in Power BI for complex multi-tenant scenarios?

**Answer:**
```dax
-- 1. Basic tenant-based RLS
-- In Sales table:
[TenantId] = USERNAME()

-- 2. User mapping based RLS (more flexible)
-- In Sales table:
VAR UserTenants = 
    CALCULATETABLE(
        VALUES(UserTenantMapping[TenantId]),
        UserTenantMapping[Username] = USERNAME(),
        UserTenantMapping[IsActive] = TRUE
    )
RETURN
[TenantId] IN UserTenants

-- 3. Role-based RLS with hierarchy
-- In Sales table:
VAR CurrentUser = USERNAME()
VAR UserRole = 
    LOOKUPVALUE(
        UserRoles[RoleName],
        UserRoles[Username], CurrentUser
    )
VAR UserTenants = 
    SWITCH(
        UserRole,
        "GlobalAdmin", VALUES(Sales[TenantId]),  -- See all tenants
        "TenantAdmin", 
            CALCULATETABLE(
                VALUES(UserTenantMapping[TenantId]),
                UserTenantMapping[Username] = CurrentUser
            ),
        "User", 
            CALCULATETABLE(
                VALUES(UserTenantMapping[TenantId]),
                UserTenantMapping[Username] = CurrentUser,
                UserTenantMapping[AccessLevel] = "READ"
            ),
        BLANK()  -- No access
    )
RETURN
[TenantId] IN UserTenants

-- 4. Time-based access control
-- In Sales table:
VAR CurrentUser = USERNAME()
VAR UserAccess = 
    CALCULATETABLE(
        UserAccess,
        UserAccess[Username] = CurrentUser,
        UserAccess[ValidFrom] <= TODAY(),
        UserAccess[ValidTo] >= TODAY()
    )
VAR AccessibleTenants = 
    SELECTCOLUMNS(UserAccess, "TenantId", UserAccess[TenantId])

RETURN
[TenantId] IN AccessibleTenants

-- 5. Department/Region based access
-- In Sales table:
VAR CurrentUser = USERNAME()
VAR UserDepartment = 
    LOOKUPVALUE(
        Users[Department],
        Users[Username], CurrentUser
    )
VAR UserRegion = 
    LOOKUPVALUE(
        Users[Region],
        Users[Username], CurrentUser
    )

RETURN
SWITCH(
    TRUE(),
    UserDepartment = "Finance", [TenantId] IN {"TENANT001", "TENANT002", "TENANT003"},
    UserDepartment = "Sales" && UserRegion = "North", [TenantId] IN {"TENANT001", "TENANT002"},
    UserDepartment = "Sales" && UserRegion = "South", [TenantId] IN {"TENANT003", "TENANT004"},
    FALSE()
)
```

---

## **Azure & Cloud Architecture Questions**

### Q8: Design a scalable Azure architecture for multi-tenant BI platform.

**Answer:**
```yaml
# Azure Resource Manager Template (ARM) snippet
{
  "resources": [
    {
      "type": "Microsoft.DataFactory/factories",
      "name": "[parameters('dataFactoryName')]",
      "properties": {
        "identity": {
          "type": "SystemAssigned"
        },
        "encryption": {
          "keyName": "[parameters('keyVaultKeyName')]",
          "keyVersion": "[parameters('keyVaultKeyVersion')]",
          "keyVaultUri": "[parameters('keyVaultUri')]"
        }
      }
    },
    {
      "type": "Microsoft.Synapse/workspaces",
      "name": "[parameters('synapseWorkspaceName')]",
      "properties": {
        "defaultDataLakeStorage": {
          "accountUrl": "[parameters('storageAccountUrl')]",
          "filesystem": "[parameters('fileSystemName')]"
        },
        "sqlAdministratorLogin": "[parameters('sqlAdminUsername')]",
        "managedResourceGroupName": "[parameters('managedResourceGroupName')]",
        "workspaceRepositoryConfiguration": {
          "type": "GitHub",
          "hostName": "github.com",
          "accountName": "[parameters('gitHubAccountName')]",
          "repositoryName": "[parameters('gitHubRepositoryName')]"
        }
      }
    }
  ]
}
```

**Architecture Components:**
```
┌─────────────────┐    ┌──────────────────┐    ┌───────────────────┐
│   Data Sources  │────│  Azure Data      │────│  Azure Data Lake  │
│  - On-premises  │    │  Factory         │    │  Storage Gen2     │
│  - SaaS APIs    │    │  - Pipelines     │    │  - Bronze/Silver/ │
│  - Files        │    │  - Triggers      │    │    Gold Layers    │
└─────────────────┘    └──────────────────┘    └───────────────────┘
                                │                        │
                                │                        │
┌─────────────────┐    ┌──────────────────┐    ┌───────────────────┐
│   Power BI      │────│  Azure Synapse   │────│  Azure Databricks │
│  - Dashboards   │    │  Analytics       │    │  - Data           │
│  - Reports      │    │  - SQL Pools     │    │    Transformation │
│  - RLS          │    │  - Serverless    │    │  - ML Models      │
└─────────────────┘    └──────────────────┘    └───────────────────┘
                                │
                                │
                    ┌──────────────────┐
                    │  Azure Key Vault │
                    │  - Secrets       │
                    │  - Certificates  │
                    │  - Access Keys   │
                    └──────────────────┘
```

### Q9: How would you implement automated scaling for multi-tenant workloads?

**Answer:**
```python
# Azure Function for dynamic scaling based on tenant usage
import azure.functions as func
import azure.mgmt.synapse as synapse_mgmt
import azure.mgmt.datafactory as adf_mgmt
from azure.identity import DefaultAzureCredential
import json
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Auto-scaling function triggered by usage metrics
    """
    try:
        # Get request data
        tenant_usage = req.get_json()
        
        # Initialize Azure clients
        credential = DefaultAzureCredential()
        synapse_client = synapse_mgmt.SynapseManagementClient(
            credential, 
            subscription_id=os.environ['SUBSCRIPTION_ID']
        )
        
        # Analyze usage patterns
        scaling_decisions = analyze_tenant_usage(tenant_usage)
        
        # Apply scaling decisions
        for decision in scaling_decisions:
            if decision['action'] == 'scale_up':
                scale_synapse_pool(synapse_client, decision)
            elif decision['action'] == 'scale_down':
                scale_down_resources(synapse_client, decision)
                
        return func.HttpResponse(
            json.dumps({"status": "success", "decisions": scaling_decisions}),
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Scaling error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=500
        )

def analyze_tenant_usage(usage_data):
    """
    Analyze tenant usage and make scaling decisions
    """
    decisions = []
    
    for tenant_id, metrics in usage_data.items():
        cpu_usage = metrics.get('cpu_percentage', 0)
        query_count = metrics.get('query_count_per_hour', 0)
        concurrent_users = metrics.get('concurrent_users', 0)
        
        # Scaling logic
        if cpu_usage > 80 or query_count > 1000 or concurrent_users > 50:
            decisions.append({
                'tenant_id': tenant_id,
                'action': 'scale_up',
                'reason': f'High usage: CPU={cpu_usage}%, Queries={query_count}',
                'target_dtu': min(metrics.get('current_dtu', 100) * 2, 1000)
            })
        elif cpu_usage < 20 and query_count < 100 and concurrent_users < 5:
            decisions.append({
                'tenant_id': tenant_id,
                'action': 'scale_down',
                'reason': f'Low usage: CPU={cpu_usage}%, Queries={query_count}',
                'target_dtu': max(metrics.get('current_dtu', 100) // 2, 50)
            })
    
    return decisions

# Terraform configuration for auto-scaling
resource "azurerm_monitor_autoscale_setting" "synapse_autoscale" {
  name                = "synapse-autoscale"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  target_resource_id  = azurerm_synapse_sql_pool.main.id

  profile {
    name = "defaultProfile"

    capacity {
      default = 2
      minimum = 1
      maximum = 10
    }

    rule {
      metric_trigger {
        metric_name        = "DWUUsedPercent"
        metric_resource_id = azurerm_synapse_sql_pool.main.id
        time_grain         = "PT1M"
        statistic          = "Average"
        time_window        = "PT5M"
        time_aggregation   = "Average"
        operator           = "GreaterThan"
        threshold          = 80
      }

      scale_action {
        direction = "Increase"
        type      = "ChangeCount"
        value     = "1"
        cooldown  = "PT5M"
      }
    }

    rule {
      metric_trigger {
        metric_name        = "DWUUsedPercent"
        metric_resource_id = azurerm_synapse_sql_pool.main.id
        time_grain         = "PT1M"
        statistic          = "Average"
        time_window        = "PT5M"
        time_aggregation   = "Average"
        operator           = "LessThan"
        threshold          = 20
      }

      scale_action {
        direction = "Decrease"
        type      = "ChangeCount"
        value     = "1"
        cooldown  = "PT5M"
      }
    }
  }
}
```

---

## **Performance Optimization Questions**

### Q10: How would you optimize a slow-performing multi-tenant query?

**Answer:**
```sql
-- Original slow query
SELECT 
    t.tenant_name,
    c.customer_name,
    p.product_name,
    SUM(f.sales_amount) as total_sales,
    COUNT(*) as transaction_count
FROM fact_sales f
    INNER JOIN dim_tenant t ON f.tenant_id = t.tenant_id
    INNER JOIN dim_customer c ON f.customer_key = c.customer_key 
        AND f.tenant_id = c.tenant_id
    INNER JOIN dim_product p ON f.product_key = p.product_key
        AND f.tenant_id = p.tenant_id
WHERE f.sale_date BETWEEN '2023-01-01' AND '2023-12-31'
    AND f.tenant_id IN ('TENANT001', 'TENANT002', 'TENANT003')
GROUP BY t.tenant_name, c.customer_name, p.product_name
ORDER BY total_sales DESC;

-- Optimized version with multiple strategies:

-- 1. Create filtered nonclustered indexes
CREATE NONCLUSTERED INDEX IX_FactSales_TenantDate_Covering
ON fact_sales (tenant_id, sale_date)
INCLUDE (customer_key, product_key, sales_amount)
WHERE sale_date >= '2023-01-01';

-- 2. Create materialized view for common aggregations
CREATE VIEW mv_sales_summary
WITH SCHEMABINDING
AS
SELECT 
    f.tenant_id,
    f.customer_key,
    f.product_key,
    YEAR(f.sale_date) as sale_year,
    MONTH(f.sale_date) as sale_month,
    SUM(f.sales_amount) as total_sales,
    COUNT_BIG(*) as transaction_count
FROM dbo.fact_sales f
WHERE f.sale_date >= '2020-01-01'  -- Only recent data
GROUP BY f.tenant_id, f.customer_key, f.product_key, 
         YEAR(f.sale_date), MONTH(f.sale_date);

CREATE UNIQUE CLUSTERED INDEX IX_SalesSummary_Clustered
ON mv_sales_summary (tenant_id, customer_key, product_key, sale_year, sale_month);

-- 3. Optimized query using materialized view
SELECT 
    t.tenant_name,
    c.customer_name,
    p.product_name,
    SUM(mv.total_sales) as total_sales,
    SUM(mv.transaction_count) as transaction_count
FROM mv_sales_summary mv
    INNER JOIN dim_tenant t ON mv.tenant_id = t.tenant_id
    INNER JOIN dim_customer c ON mv.customer_key = c.customer_key 
        AND mv.tenant_id = c.tenant_id AND c.is_current = 1
    INNER JOIN dim_product p ON mv.product_key = p.product_key
        AND mv.tenant_id = p.tenant_id
WHERE mv.sale_year = 2023
    AND mv.tenant_id IN ('TENANT001', 'TENANT002', 'TENANT003')
GROUP BY t.tenant_name, c.customer_name, p.product_name
ORDER BY total_sales DESC
OPTION (MAXDOP 4);

-- 4. Partitioned view for very large datasets
CREATE VIEW vw_fact_sales_partitioned AS
SELECT * FROM fact_sales_2023
WHERE sale_date BETWEEN '2023-01-01' AND '2023-12-31'
UNION ALL
SELECT * FROM fact_sales_2024
WHERE sale_date BETWEEN '2024-01-01' AND '2024-12-31';

-- 5. Query hints and optimization
SELECT 
    t.tenant_name,
    c.customer_name,
    p.product_name,
    SUM(f.sales_amount) as total_sales,
    COUNT(*) as transaction_count
FROM fact_sales f WITH (INDEX(IX_FactSales_TenantDate_Covering))
    INNER JOIN dim_tenant t WITH (NOLOCK) ON f.tenant_id = t.tenant_id
    INNER JOIN dim_customer c WITH (NOLOCK) ON f.customer_key = c.customer_key 
        AND f.tenant_id = c.tenant_id
    INNER JOIN dim_product p WITH (NOLOCK) ON f.product_key = p.product_key
        AND f.tenant_id = p.tenant_id
WHERE f.sale_date BETWEEN '2023-01-01' AND '2023-12-31'
    AND f.tenant_id IN ('TENANT001', 'TENANT002', 'TENANT003')
GROUP BY t.tenant_name, c.customer_name, p.product_name
ORDER BY total_sales DESC
OPTION (HASH JOIN, MAXDOP 4, USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE'));
```

These technical questions cover the core areas that interviewers typically focus on for Multi-Tenant BI Platform projects. They test deep understanding of database design, ETL processes, security implementation, performance optimization, and cloud architecture.