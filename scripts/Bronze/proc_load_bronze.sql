/*
==============================================================================
Stored procedure: Load Bronze Layer (Source -> Bronze)
==============================================================================
Scripts Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    it performs the following action:
        - Truncates the bronze table before loading data.
        - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

        
Parametrs:
    NONE.
    This stored procedure dose not accept any parameters or return any values.

Usage Example:
    EXCE bronze.load_bronze;

===============================================================================
*/





create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime,@end_time datetime ,@batch_start_time datetime,@batch_end_time datetime;

	BEGIN TRY
		set @batch_start_time=getdate();
		print 'Loading Bronze layer';
		print '======================================================================'
		print'Loading CRM Tables'
		print '======================================================================'
		
		set @start_time=GETDATE();
		print 'Tuncating table: bronze.crm_cust_info '
		truncate table bronze.crm_cust_info

		print 'Inserting Data into: bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info
		from
		'C:\Users\praka\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator=','
		);
		set @end_time=getdate();
		print '>> LOAD DURATION:' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>-------------------'

		set @start_time=getdate()
		print 'Tuncating table: bronze.crm_prd_info '
		truncate table bronze.crm_prd_info

		print 'Inserting Data into: bronze.crm_prd_info'
		bulk insert bronze.crm_prd_info
		from 'C:\Users\praka\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with( 
			firstrow=2,
			fieldterminator=','
		);
		set @end_time=getdate();
		print '>>LOAD DURATION ' + CAST(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
		print'>>-------------------'

		set @start_time=getdate();
		print 'Tuncating table: bronze.crm_sales_details '
		truncate table bronze.crm_sales_details

		print 'Inserting Data into: bronze.crm_sales_details'
		bulk insert bronze.crm_sales_details
		from 'C:\Users\praka\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow=2,
			fieldterminator=','
		);
		set @end_time=getdate();
		print '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +' seconds'
		print'>>-------------------'


		print '======================================================================'
		print'Loading ERP Tables'
		print '======================================================================'
		
		set @start_time=getdate();
		print 'Tuncating table: bronze.erp_cust_az12 '
		truncate table bronze.erp_cust_az12
		print 'Inserting Data into: bronze.erp_cust_az12'
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\praka\Documents\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
		firstrow=2,
		fieldterminator=','
		);
		set @end_time=getdate()
		print '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +' seconds'
		print'>>-------------------'

		set @start_time=getdate();
		print 'Tuncating table: bronze.erp_loc_a101 '
		truncate table bronze.erp_loc_a101
		print 'Inserting Data into: bronze.erp_loc_a101'

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\praka\Documents\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
		firstrow=2,
		fieldterminator=','
		);
		set @end_time=getdate();
		print '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +' seconds'
		print'>>-------------------'

		set @start_time=getdate();
		print 'Tuncating table: bronze.erp_px_cat_g1v2 '
		truncate table bronze.erp_px_cat_g1v2
		print 'Inserting Data into: bronze.erp_px_cat_g1v2'
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\praka\Documents\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
		firstrow=2,
		fieldterminator=','
		);
		set @end_time=getdate();
		print '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +' seconds'
		print'>>-------------------'

		set @batch_end_time=getdate();
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
		print 'LOADING BRONZE LAYER IS COMPLETED';
		PRINT 'Total load Duration:' + cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+'  seconds'
		print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

	END TRY
	BEGIN CATCH
		PRINT '======================================================================'
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		print '======================================================================'
	END CATCH

end
