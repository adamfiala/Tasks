 /* First step - manually upload all the CSV files into dbsix database */
 /* Second step - merge all the tables into one, save the table */

select * 
into dbsix.dbo.merged
from dbsix.dbo.JetBrainsAmericasUSD_settlement_detail_report_batch_138

union
select * from dbsix.dbo.JetBrainsAmericasUSD_settlement_detail_report_batch_139
union
select * from dbsix.dbo.JetBrainsAmericasUSD_settlement_detail_report_batch_140
union
select * from dbsix.dbo.JetBrainsAmericasUSD_settlement_detail_report_batch_141
union
select * from dbsix.dbo.JetBrainsAmericasUSD_settlement_detail_report_batch_142

union
select * from dbsix.dbo.JetBrainsEUR_settlement_detail_report_batch_138
union
select * from dbsix.dbo.JetBrainsEUR_settlement_detail_report_batch_139
union
select * from dbsix.dbo.JetBrainsEUR_settlement_detail_report_batch_140
union
select * from dbsix.dbo.JetBrainsEUR_settlement_detail_report_batch_141
union
select * from dbsix.dbo.JetBrainsEUR_settlement_detail_report_batch_142

union
select * from dbsix.dbo.JetBrainsGBP_settlement_detail_report_batch_138
union
select * from dbsix.dbo.JetBrainsGBP_settlement_detail_report_batch_139
union
select * from dbsix.dbo.JetBrainsGBP_settlement_detail_report_batch_140
union
select * from dbsix.dbo.JetBrainsGBP_settlement_detail_report_batch_141
union
select * from dbsix.dbo.JetBrainsGBP_settlement_detail_report_batch_142

union
select * from dbsix.dbo.JetBrainsUSD_settlement_detail_report_batch_138
union
select * from dbsix.dbo.JetBrainsUSD_settlement_detail_report_batch_139
union
select * from dbsix.dbo.JetBrainsUSD_settlement_detail_report_batch_140
union
select * from dbsix.dbo.JetBrainsUSD_settlement_detail_report_batch_141
union
select * from dbsix.dbo.JetBrainsUSD_settlement_detail_report_batch_142
 
 /* Sum and group the amount, one row for one order_ref - 
	Now we have all data we need from payment gateways in one table */ 

 select 
	 a.COMPANY_ACCOUNT, 
	 a.MERCHANT_ACCOUNT, 
	 a.BATCH_NUMBER, 
	 a.ORDER_REF, 
	 SUM(convert(numeric,net)) as net,
	 SUM(convert(numeric,fee)) as fees,
	 SUM(convert(numeric,gross)) as gross

 into dbsix.dbo.merged_gateways

 from dbsix.dbo.merged a
 group by 
	 a.COMPANY_ACCOUNT, 
	 a.MERCHANT_ACCOUNT, 
	 a.BATCH_NUMBER, 
	 a.ORDER_REF

/* Next step - prepare the Netsuite data from database */

  select 
      a.MERCHANT_ACCOUNT, 
	  a.BATCH_NUMBER, 
	  a.order_ref, 
	  sum(case 
			when a.TRANSACTION_TYPE = 'Payment' then amount_foreign
			when a.TRANSACTION_TYPE = 'Customer Deposit' then amount_foreign 
			else 0 end
		) as amount_foreign_total,
	  sum(case 
			when a.TRANSACTION_TYPE = 'Payment' and b.account_id <> 2274 then amount_foreign
			when a.TRANSACTION_TYPE = 'Customer Deposit' and b.account_id <> 2274 then amount_foreign 
			else 0 end
		) as amount_foreign_net,
	  sum(case 
			when a.TRANSACTION_TYPE = 'Payment' and b.account_id = 2274 then amount_foreign
			when a.TRANSACTION_TYPE = 'Customer Deposit' and b.account_id = 2274 then amount_foreign 
			else 0 end
		) as amount_foreign_fees
		 
  into dbsix.dbo.payments_netsuite

  from bi.netsuite.TRANSACTIONS a
  left join bi.netsuite.TRANSACTION_LINES b on a.TRANSACTION_ID = b.TRANSACTION_ID
  where 
	b.ACCOUNT_ID > 2000 /* Only accounts listed */
    and (a.TRANSACTION_TYPE = ('Payment') or  a.TRANSACTION_TYPE = 'Customer Deposit') /* Calculate only these types */

  group by a.order_ref, a.MERCHANT_ACCOUNT, a.BATCH_NUMBER
  order by a.order_ref, a.MERCHANT_ACCOUNT, a.BATCH_NUMBER

/* Next step - merge Netsuite and Payment gateways tables with full join */

  select 
	  COMPANY_ACCOUNT,
      a.MERCHANT_ACCOUNT,
      a.BATCH_NUMBER,
      a.ORDER_REF,
      coalesce(GROSS,0) as Amount_Payment_Gateway,
	  coalesce(b.AMOUNT_FOREIGN_TOTAL,0) as Amount_NetSuite,
	  coalesce(NET,0) as Amount_Payment_Gateway_NET,
	  coalesce(b.AMOUNT_FOREIGN_NET,0) as Amount_NetSuite_Net,
	  coalesce(FEES,0) as Amount_Payment_Gateway_FEES,
	  coalesce(b.AMOUNT_FOREIGN_FEES,0) as Amount_NetSuite_Fees

  into dbsix.dbo.merged_netsuite_gateways

  from dbsix.dbo.merged_gateways a
  full outer join dbsix.dbo.payments_netsuite b on a.order_ref = b.order_ref  and a.MERCHANT_ACCOUNT = b.MERCHANT_ACCOUNT and a.batch_number = b.batch_number
  order by a.ORDER_REF

 /*  This is just a control summary - same data as in the table on Github - checked  */

  select 
       a.MERCHANT_ACCOUNT, 
	   a.BATCH_NUMBER, 
	   SUM(convert(numeric,Amount_Payment_Gateway)) as Amount_Payment_Gateway, 
	   SUM(Amount_NetSuite) as Amount_NetSuite,
	   SUM(Amount_NetSuite) - SUM(convert(numeric,Amount_Payment_Gateway)) as difference

  from dbsix.dbo.merged_netsuite_gateways a
  group by  a.MERCHANT_ACCOUNT, a.BATCH_NUMBER
  order by  a.MERCHANT_ACCOUNT, a.BATCH_NUMBER
 /*
 We can see the difference of null batch numbers (1st row) is equal of difference USD batch 139 + 141.
 That means only batch numbers are not filled in Netsuite for those transations.
 Those transactions are now duplicated in this table (once with filled batch, one without it). I need to filter the duplicates in next step.
 */

  /* Here is final table with all transactions with Netsuite/Gateway difference */

  select 
       MERCHANT_ACCOUNT, 
	   BATCH_NUMBER, 
	   COMPANY_ACCOUNT, 
	   ORDER_REF, 
	   Amount_NetSuite, 
	   Amount_Payment_Gateway,
	   Amount_NetSuite - (convert(numeric,Amount_Payment_Gateway)) as Total_difference_between_systems,
	   Amount_Payment_Gateway_NET,
	   Amount_NetSuite_Net,
	   Amount_Payment_Gateway_Fees,
	   Amount_NetSuite_Fees,
	   case 
		   when Amount_Payment_Gateway_Fees <> Amount_NetSuite_Fees and Amount_Payment_Gateway_NET = Amount_NetSuite_Net then 'Difference in Fee value'
		   when Amount_Payment_Gateway_Fees = Amount_NetSuite_Fees and Amount_Payment_Gateway_NET <> Amount_NetSuite_Net then 'Difference in NET value'
		   when ((BATCH_NUMBER = 139 or BATCH_NUMBER = 141) and MERCHANT_ACCOUNT = 'JetBrainsAmericasUSD') then concat('Missing Batch number ',BATCH_NUMBER, ' on Netsuite platform') 
		   /* In this case, correct financial data are in Netsuite, just the batch number is missing */
		   else 'Difference in either NET and FEE values'
		end as Source_of_problem

  into dbsix.dbo.task_1_results

  from dbsix.dbo.merged_netsuite_gateways
  where (convert(numeric,Amount_Payment_Gateway)) - Amount_NetSuite <> 0 
  and BATCH_NUMBER is not null /* Values with missing batch number 139 and 141, filtering out the duplicates */
  order by MERCHANT_ACCOUNT, BATCH_NUMBER


