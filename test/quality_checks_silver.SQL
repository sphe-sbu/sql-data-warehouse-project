/*
=============================================================================
Quality Checks
=============================================================================
Script Purpose:
    This script perfoms various quality checks for data consistency, accuracy,
    and starndardization across the 'Silver' schemas. It includes checks for:
      - NULL or duplicates primary keys.
      - Unwanted spaces in string fields.
      - Data starndardization and consistency.
      - Invalid date ranges and orders.
      - Data consistency between related fields.

Usage Notes:
      - Run these checks after data loading Silver layer.
      - Investigate and resolver any discrepancies found during the checks.
=============================================================================
/*

--- =========================================================================
---  Checking 'silver.crm_cust_info'
--- =========================================================================

--- Check NULLS or duplicates in the primary key
--- Expectations: No results

