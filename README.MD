# TechMart E-Commerce Analytics - Holistics Data Modeling Documentation

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture & Data Flow](#architecture--data-flow)
3. [Dataset Design](#dataset-design)
4. [Role-Playing Dimensions](#role-playing-dimensions)
5. [AQL Metrics & Calculations](#aql-metrics--calculations)
6. [Data Quality Framework](#data-quality-framework)
7. [Technical Documentation](#technical-documentation)
8. [Assumptions & Design Decisions](#assumptions--design-decisions)

---

## Project Overview

This project implements a comprehensive Business Intelligence solution for **TechMart**, a mid-sized e-commerce company, using Holistics as the BI platform. The solution addresses key business requirements including:

- **Sales Performance Analysis**: Revenue, order counts, customer behavior
- **Inventory Management**: Stock levels, turnover rates, availability tracking
- **Customer Analytics**: Segmentation, retention, purchase frequency
- **Multi-Date Analysis**: Order lifecycle tracking (created, shipped, delivered, cancelled)

**Time Given**: 3 days  
**Deadline**: Sunday, 17th January 2026 at 12:00 pm (Guatemala time)

---

## Architecture & Data Flow

### Medallion Architecture

The project follows a **Bronze → Silver → Gold** medallion architecture pattern:

```
┌─────────────┐
│   Source    │  (ecommerce schema)
│   Database  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   BRONZE    │  Raw data extraction & basic joins
│   Layer     │  - fct_orders (order_id + product_id grain)
│             │  - dim_* tables (users, products, categories, etc.)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Data cleaning, deduplication, business rules
│   Layer     │  - Conflict resolution (delivered_at vs cancelled_at)
│             │  - Status normalization
│             │  - Surrogate key generation
│             │  - Data quality flags
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Business-ready datasets
│   Layer     │  - gld_fct_order_lines (line-level analytics)
│             │  - gld_fct_orders (order-level aggregation)
│             │  - gld_fct_inventory_current (current stock)
│             │  - gld_fct_inventory_daily (historical snapshots)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Holistics  │  Semantic layer, metrics, dashboards
│  Datasets   │
└─────────────┘
```

### Layer Responsibilities

#### Bronze Layer
- **Purpose**: Raw data extraction from source system
- **Grain**: Preserves original source grain
- **Key Models**:
  - `fct_orders`: Joins `order_items`, `orders`, and `products` at (order_id, product_id) grain
  - `dim_*`: Dimension tables as-is from source

#### Silver Layer
- **Purpose**: Data quality, cleaning, and business rule application
- **Key Transformations**:
  - **Conflict Resolution**: Handles contradictory timestamps (e.g., delivered_at and cancelled_at both present)
  - **Status Normalization**: Derives consistent order_status from timestamps
  - **Deduplication**: Removes duplicate rows at correct grain
  - **Surrogate Keys**: Generates stable MD5-based keys
  - **Data Quality Flags**: Tracks data issues for monitoring

#### Gold Layer
- **Purpose**: Business-ready fact tables optimized for analytics
- **Key Models**:
  - `gld_fct_order_lines`: Line-level fact with role-playing date keys
  - `gld_fct_orders`: Order-level aggregations
  - `gld_fct_inventory_current`: Latest inventory snapshot per product
  - `gld_fct_inventory_daily`: Historical inventory snapshots

---

## Dataset Design

### Challenge 1.1: Unified Sales & Inventory Dataset

**Business Requirement**: "We need a single dataset where our business users can analyze both sales performance and inventory levels. They should be able to see metrics like revenue by product category, but also inventory turnover rates."

#### Solution Design

We create a **unified dataset** that includes both order facts and inventory facts, connected through shared dimensions. This allows users to:

1. Analyze sales metrics (revenue, order counts) by product, category, merchant
2. Analyze inventory metrics (stock levels, turnover) by product
3. Combine both (e.g., "Which products have high revenue but low inventory turnover?")


---

## Data Quality Framework

### Data Quality Checks

The project includes comprehensive data quality queries in the `Queries` folder:

#### 1. Completeness Checks
- Null ID checks
- Missing required fields
- Empty string validation

#### 2. Uniqueness Checks
- Duplicate ID detection
- Duplicate email detection (in users)

#### 3. Referential Integrity
- Orphan foreign keys (e.g., parent_id references that don't exist)
- Self-referencing cycles (e.g., category A → B and B → A)

#### 4. Business Rule Validation
- Negative prices
- Invalid price ranges (min_price > max_price)
- Extreme value detection

## Assumptions & Design Decisions

### Data Assumptions

1. **Order Status Logic**: 
   - An order can be refunded after delivery (refunded_at can coexist with delivered_at)
   - An order cannot be both delivered and cancelled (conflict resolution in Silver layer)

2. **Inventory Snapshots**: 
   - Inventory data is provided as snapshots (not real-time)
   - Latest snapshot represents "current" inventory

3. **Email Uniqueness**: 
   - Email addresses should be unique per customer
   - Duplicate emails indicate data quality issues (tracked in Silver layer)

4. **Date Handling**: 
   - All dates are stored as DATE or TIMESTAMP
   - Timezone is assumed to be consistent (UTC or business timezone)

### Design Decisions

1. **Medallion Architecture**: Chosen for data quality, auditability, and incremental processing capabilities.

2. **Surrogate Keys**: MD5-based keys for stability and performance, even if source IDs change.

3. **Grain Preservation**: Maintain line-level grain in Gold layer to support detailed analysis while providing order-level aggregations.

4. **Role-Playing Dimensions via `extend()`**: Chosen for better user experience over `with_relationships()`.

5. **Separate Inventory Facts**: Current and daily inventory kept separate to optimize for different query patterns.

### Future Enhancements

1. **Incremental Processing**: Add incremental load logic to Bronze/Silver layers
2. **Data Lineage**: Implement data lineage tracking for auditability
3. **Advanced Segmentation**: Add RFM (Recency, Frequency, Monetary) analysis
4. **Forecasting**: Add time-series forecasting for inventory and revenue
5. **Real-time Inventory**: If real-time inventory becomes available, add streaming fact table

---

## File Structure

```
Holistics/
├── Queries/
│   │
│   ├──    # Data quality validation
│   │   ├── orders.sql
│   │   ├── users.sql
│   │   ├── products.sql
│   │   └── categories.sql
├── models/
│   │
│   ├── Silver/              # Data cleaning & business rules
│   │   ├── orders.sql       # Conflict resolution, status normalization
│   │   ├── users.sql        # Email deduplication
│   │   ├── products.sql
│   │   ├── categories.sql
│   │   ├── inventory.sql
│   │   ├── dates.sql
│   │   └── ...
│   │
│   └── Gold/                # Business-ready datasets
│       ├── order_lines.sql  # Line-level fact with date keys
│       ├── orders.sql       # Order-level aggregations
│       ├── inventory_current.sql
│       └── inventory_daily.sql
│
├── datasets/                # Holistics dataset definitions (AML)
│   └── (dataset configuration files)
│
└── README.MD               # This file
```

---

## Conclusion

This documentation provides a comprehensive overview of the TechMart e-commerce analytics solution built on Holistics. The architecture follows industry best practices for data modeling, ensuring scalability, maintainability, and user-friendly analytics.

For questions or clarifications, please refer to the Holistics documentation or contact the data team.

**Last Updated**: January 2026  
**Version**: 1.0

