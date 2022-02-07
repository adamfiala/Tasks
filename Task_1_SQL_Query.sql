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
 
 /* Now we have all important data we need from payment gateways in one table.
 I splitted fees and net payments, so it the same as in Netsuite platform
 */ 

 select 
	 a.COMPANY_ACCOUNT, 
	 a.MERCHANT_ACCOUNT, 
	 a.BATCH_NUMBER, 
	 a.ORDER_REF, 
	 A.type,
	 convert(date,A.date) as date,
	 convert(numeric,net) as amount_foreign

 into dbsix.dbo.merged_gateways

 from dbsix.dbo.merged a

 union 

 select 
	 a.COMPANY_ACCOUNT, 
	 a.MERCHANT_ACCOUNT, 
	 a.BATCH_NUMBER, 
	 a.ORDER_REF, 
	 A.type,
	 convert(date,A.date) as date,
	 convert(numeric,fee) as amount_foreign

 from dbsix.dbo.merged a


/* Next step - prepare the Netsuite data from database */

  select 
      a.MERCHANT_ACCOUNT, 
	  a.BATCH_NUMBER, 
	  a.order_ref, 
	  a.TRANSACTION_TYPE,
	  a.TRANDATE,
	  AMOUNT_FOREIGN,
	  AMOUNT
		 
  into dbsix.dbo.payments_netsuite

  from bi.netsuite.TRANSACTIONS a
  left join bi.netsuite.TRANSACTION_LINES b on a.TRANSACTION_ID = b.TRANSACTION_ID
  where 
	b.ACCOUNT_ID > 2000 /* Only accounts listed */
    and (a.TRANSACTION_TYPE = ('Payment') or  a.TRANSACTION_TYPE = 'Customer Deposit') /* Calculate only these types */

  order by a.order_ref, a.MERCHANT_ACCOUNT, a.BATCH_NUMBER

/* Next step - merge Netsuite and Payment gateways tables with full join */

  select 
	 a.COMPANY_ACCOUNT, 
	 a.MERCHANT_ACCOUNT, 
	 a.BATCH_NUMBER, 
	 a.ORDER_REF, 
	 A.type,
	 a.date,
	 b.TRANSACTION_TYPE,
	 b.TRANDATE,
	 case 
	 when b.order_ref IS null then 'Transaction missing in Netsuite' 
	 when a.order_ref IS null then 'Transaction missing in Payment Gateway'
	 when b.BATCH_NUMBER IS null then concat('Missing Batch number ',a.BATCH_NUMBER, ' on Netsuite platform') 
	 when a.date <> b.trandate and b.trandate is not null then concat('Wrong transaction date on Netsuite. Correct date: ', a.date)
	 end as Reason,
	 coalesce(a.amount_foreign,0) as amount_gateways,
	 coalesce(b.amount_foreign,0) as amount_netsuite,
	 coalesce(b.amount_foreign,0) - coalesce(a.amount_foreign,0) as difference

  into dbsix.dbo.merged_netsuite_gateways

  from dbsix.dbo.merged_gateways a
  full outer join dbsix.dbo.payments_netsuite b on a.order_ref = b.order_ref and a.amount_foreign = b.amount_foreign 
  order by a.ORDER_REF

 /*  This is just a control summary */

  select 
       a.MERCHANT_ACCOUNT, 
	   a.BATCH_NUMBER, 
	   SUM(amount_gateways) as Amount_Payment_Gateway, 
	   SUM(amount_netsuite) as Amount_NetSuite,
	   SUM(Amount_NetSuite) - SUM(amount_gateways) as difference

  from dbsix.dbo.merged_netsuite_gateways a
  group by  a.MERCHANT_ACCOUNT, a.BATCH_NUMBER
  order by  a.MERCHANT_ACCOUNT, a.BATCH_NUMBER

  /* Here is final table with all transactions with Netsuite/Gateway difference and its reasons (there are only 2 reasons) */
  /* US - 139,141 - missing batch number in Netsuite
     EU - 138,139 - missing some transactions in Netsuite, either payments and refunds
	 EU - 140 - wrong transaction dates on Netsuite
	 GB - 141 - missing ALL transactions in Netsuite at all
	 */
  select 
	 Company_account, 
	 Merchant_account, 
	 Batch_number, 
	 Order_ref, 
	 Type as Transaction_type_Gateway,
	 Date as Date_in_Gateway,
	 Transaction_type as Transaction_type_Netsuite,
	 Trandate as Date_in_Netsuite,
  	 Amount_gateways as Amount_in_Gateway,
	 Amount_netsuite as Amount_in_Netsuite,
	 Difference as Difference_between_systems,
	 reason as Source_of_problem

  into dbsix.dbo.task_1_results

  from dbsix.dbo.merged_netsuite_gateways
  where reason is not null
  order by merchant_account, batch_number


