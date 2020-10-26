#' Load All Data
#'
#' This function is designed to be called with the list of data you need to load into the data, this will pull the releveant data, check if it's been loaded
#' then load it to the DB, There are lots of parameters that you need to pass to make sure the tables are named correctly.
#' search for a specific book if passed
#' @param user username for the api
#' @param password password for the website
#' @param string Fragment of Book name to search for
#' @return
#' @export

get_and_load_book_data <- function(
  completed_book_df,
  db_un,
  db_pw,
  db_driver,
  db_server,
  db_name,
  db_port,
  db_schema,
  api_un,
  api_pw,
  slice_amount = 1000,
  core_info_table_name = 'OcrolusBooksCoreInfo',
  activity_info_active_table_name = 'OcrolusBooksActivityInfoActive',
  activity_info_missing_table_name = 'OcrolusBooksActivityInfoMissing',
  periods_table_name = 'OcrolusBooksPeriods',
  txns_table_name = 'OcrolusBooksTransactions',
  interbank_table_name = 'OcrolusAnalyticsInterbankTxns',
  alt_lender_table_name = 'OcrolusAnalyticsAlternativeLenderTxs',
  round_number_txs_table_name = 'OcrolusAnalyticsRoundNumberTxns',
  ppp_loan_txn_table_name = 'OcrolusAnalyticsPPPLoanTxns',
  nsf_txns_table_name = 'OcrolusAnalyticsNSFTxns',
  period_balance_mismatches_table_name = 'OcrolusAnalyticsPeriodBalanceMismatches',
  outside_source_deposits_table_name = 'OcrolusAnalyticsOutsideSourceDeposits',
  withdrawals_max_by_month_table_name = 'OcrolusAnalyticsWithdrawalsMaxByMonth',
  deposit_min_by_month_table_name = 'OcrolusAnalyticsDepositMinByMonth',
  deposit_max_by_month_table_name = 'OcrolusAnalyticsDepositMaxByMonth',
  negative_balances_by_month_table_name = 'OcrolusAnalyticsNegativeBalancesByMonth',
  withdrawals_sum_by_month_table_name = 'OcrolusAnalyticsWithdrawalsSumByMonth',
  daily_cash_flows_table_name = 'OcrolusAnalyticsDailyCashFlows',
  deposits_sum_by_month_table_name = 'OcrolusAnalyticsDepositsByMonth',
  estimated_revenue_by_month_table_name = 'OcrolusAnalyticsEstimatedRevenueByMonth',
  daily_balances_table_name = 'OcrolusAnalyticsDailyBalances',
  completed_table_name = 'OcrolusLoadedIDs') {

  completed_book_df = completed_book_df %>% select(id,bk_pk) %>% distinct
  completed_book_df = completed_book_df %>% group_by(id) %>% nest()
  slice_amount = slice_amount
  while (T){
    connection = get_sql_connection(user_name = db_un,password = db_pw,driver_name = db_driver,server = db_server,database = db_name,port = db_port)

    if(exists('completed_ids')){
      loaded_ids = completed_ids %>% select(id) %>% pull
      completed_book_df = completed_book_df %>% filter(!(id %in% loaded_ids))
    } else{
      loaded_ids = get_all_data_from_table(connection,completed_table_name,db_schema) %>% select(id) %>% pull
      completed_book_df = completed_book_df %>% filter(!(id %in% loaded_ids))

    }

    remaining_rows = nrow(df_status_book_complete_info)

    if (remaining_rows == 0){
      print('No more to load')
      break
    }
    if (remaining_rows < slice_amount) slice_amount = remaining_rows

    next_load<-completed_book_df %>%ungroup %>%  slice(1:slice_amount)

    next_load_book_info = next_load %>% mutate(
      book_info = map(data,~get_book_info(user,password,.$bk_pk))
    )
    next_load_book_info = next_load_book_info %>%
      mutate(
        details = map(book_info,~extract_bank_accounts_details(.x) )
      )
    core_info = extract_nested_df(next_load_book_info,details,'core_info')
    core_info_key_cols = c('id','pk','bk_id')
    load_book_information(core_info,connection,core_info_table_name,db_schema,core_info_key_cols)

    activity_info_active = extract_nested_df(next_load_book_info,details,'activity_info_active' )
    activity_info_active_key_cols = c('id','name','start_day','start_month','end_year','end_day','end_month')
    load_book_information(activity_info_active,connection,activity_info_active_table_name,db_schema,activity_info_active_key_cols)

    activity_info_missing = extract_nested_df(next_load_book_info,details,'activity_info_missing' )
    activity_info_missing_key_cols = c('id','name','start_day','start_month','end_year','end_day','end_month')
    load_book_information(activity_info_missing,connection,activity_info_missing_table_name,db_schema,activity_info_missing_key_cols)


    periods = extract_nested_df(next_load_book_info,details,'periods' )
    periods_key_cols = c('id','name','pk')
    load_book_information(periods,connection,periods_table_name,db_schema,periods_key_cols)


    next_load_book_info <- next_load_book_info %>% mutate(
      txs = map(data, ~get_transactions(user,password,.$bk_pk))
    )

    next_load_book_info <- next_load_book_info %>%
      mutate(
        tx_details_response = map(txs,~pluck_map(.x,'response'))
        ,tx_details = map(tx_details_response,~pluck_map(.x,'txns'))
      )

    txs = next_load_book_info %>% unnest(cols = tx_details) %>% select_if(~!(is.list(.x)))
    txns_key_cols = c('id','pk')
    load_book_information(txs,connection,txns_table_name,db_schema,txns_key_cols)

    next_load_book_info <- next_load_book_info %>% mutate(
      analytics = map(data, ~get_analytics(user,password,.$bk_pk))
    )

    next_load_book_info <- next_load_book_info %>% mutate(
      analytics_response =   map(analytics,~pluck_map(.x,'response'))
      ,average_deposit_by_month = map(analytics_response , ~enframe_monthly_analyics(.x,'average_deposit_by_month'))
      ,estimated_revenue_by_month = map(analytics_response , ~enframe_monthly_analyics(.x,'estimated_revenue_by_month'))
      ,average_deposit_by_month = map(analytics_response , ~enframe_monthly_analyics(.x,'average_deposit_by_month'))
      ,average_daily_balance_by_month = map(analytics_response , ~enframe_monthly_analyics(.x,'average_daily_balance_by_month'))
      ,bank_accts = map(analytics_response, ~pluck_map(analytics_response, 'bank_accounts'))

    )

    next_load_book_info <- next_load_book_info %>% mutate(
      bank_accts = map(analytics_response, ~pluck(.x,'bank_accounts')))

    next_load_book_info <- next_load_book_info %>% mutate(
      analytics_response =   map(analytics,~pluck_map(.x,'response'))
      ,bank_accts = get_bank_account_from_analyics(analytics_response)
      ,interbank_txs = map(bank_accts, ~pluck_map_df(.x,'interbank_transactions'))
      ,alternative_lender_transactions = map(bank_accts, ~pluck_map(.x,'alternative_lender_transactions'))
      ,round_number_txns = map(bank_accts, ~pluck_map(.x,'round_number_txns'))
      ,ppp_loan_txns = map(bank_accts, ~pluck_map(.x,'ppp_loan_txns'))
      ,nsf_transactions = map(bank_accts, ~pluck_map(.x,'nsf_transactions'))
      ,period_balance_mismatches  = map(bank_accts, ~pluck_map(.x,'period_balance_mismatches'))
      ,outside_source_deposits  = map(bank_accts, ~pluck_map(.x,'outside_source_deposits'))
      ,withdrawals_max_by_month  = map(bank_accts, ~get_min_max_by_month(.x,'withdrawals_max_by_month'))
      ,deposit_min_by_month  = map(bank_accts, ~get_min_max_by_month(.x,'deposit_min_by_month'))
      ,deposit_max_by_month  = map(bank_accts, ~get_min_max_by_month(.x,'deposit_max_by_month'))
      ,negative_balances_by_month  = map(bank_accts, ~get_negative_balance(.x,'negative_balances_by_month'))
      ,withdrawals_sum_by_month = map(bank_accts, ~get_txn_info_single_row(.x,'withdrawals_sum_by_month'))
      ,daily_cash_flows = map(bank_accts, ~get_txn_info_single_row(.x,'daily_cash_flows'))
      ,deposits_sum_by_month = map(bank_accts, ~get_txn_info_single_row(.x,'deposits_sum_by_month'))
      ,estimated_revenue_by_month = map(bank_accts, ~get_txn_info_single_row(.x,'estimated_revenue_by_month'))
      ,daily_balances = map(bank_accts, ~get_txn_info_single_row(.x,'daily_balances'))
    )



    interbank_txs_df = unnest_analytics_df(next_load_book_info,interbank_txs)
    interbank_key_cols = c('id','page_idx','bank_account_pk','page_doc_pk','pk','uploaded_doc_pk')
    load_book_information(interbank_txs_df,connection,interbank_table_name,db_schema,interbank_key_cols,analytics = T)

    alternative_lender_transactions_df = unnest_analytics_df(next_load_book_info,alternative_lender_transactions)
    alt_lender_key_cols = c('id','page_idx','bank_account_pk','page_doc_pk','pk','uploaded_doc_pk')
    load_book_information(alternative_lender_transactions_df,connection,alt_lender_table_name,db_schema,alt_lender_key_cols,analytics = T)


    round_number_txns_df = unnest_analytics_df(next_load_book_info,round_number_txns)
    round_number_txs_key_cols = c('id','name','pk')
    load_book_information(round_number_txns_df,connection,round_number_txs_table_name,db_schema,round_number_txs_key_cols,analytics = T)

    ppp_loan_txns_df = unnest_analytics_df(next_load_book_info,ppp_loan_txns)
    ppp_loan_key_cols = c('id','name','pk')
    load_book_information(ppp_loan_txns_df,connection,ppp_loan_txn_table_name,db_schema,ppp_loan_key_cols,analytics = T)

    nsf_transactions_df = unnest_analytics_df(next_load_book_info,nsf_transactions)
    nsf_txns_key_cols = c('id','name','pk')
    load_book_information(nsf_transactions_df,connection,nsf_txns_table_name,db_schema,nsf_txns_key_cols,analytics = T)

    period_balance_mismatches_df = unnest_analytics_df(next_load_book_info,period_balance_mismatches)
    period_balance_mismatches_key_cols = c('id','page_idx','bank_account_pk','page_doc_pk','pk','uploaded_doc_pk')
    load_book_information(period_balance_mismatches_df,connection,period_balance_mismatches_table_name,db_schema,period_balance_mismatches_key_cols,analytics = T)

    outside_source_deposits_df = unnest_analytics_df(next_load_book_info,outside_source_deposits)
    outside_source_deposits_key_cols =c('id','page_idx','bank_account_pk','page_doc_pk','pk','uploaded_doc_pk')
    load_book_information(outside_source_deposits_df,connection,period_balance_mismatches_table_name,db_schema,outside_source_deposits_key_cols,analytics = T)

    withdrawals_max_by_month_df = unnest_analytics_df(next_load_book_info,withdrawals_max_by_month) %>% remove_empty()
    withdrawals_max_by_month_key_cols = c('id','uploaded_doc_pk','pk','page_doc_pk','bank_account_pk')
    load_book_information(withdrawals_max_by_month_df,connection,withdrawals_max_by_month_table_name,db_schema,withdrawals_max_by_month_key_cols,analytics = T)

    deposit_min_by_month_df = unnest_analytics_df(next_load_book_info,deposit_min_by_month)
    deposit_min_by_month_key_cols = c('id','uploaded_doc_pk','pk','page_doc_pk','bank_account_pk')
    load_book_information(deposit_min_by_month_df,connection,deposit_min_by_month_table_name,db_schema,deposit_min_by_month_key_cols,analytics = T)

    deposit_max_by_month_df = unnest_analytics_df(next_load_book_info,deposit_max_by_month)
    deposit_max_by_month_key_cols = c('id','uploaded_doc_pk','pk','page_doc_pk','bank_account_pk')
    load_book_information(deposit_max_by_month_df,connection,deposit_max_by_month_table_name,db_schema,deposit_max_by_month_key_cols,analytics = T)

    negative_balances_by_month_df = unnest_analytics_df(next_load_book_info,negative_balances_by_month) %>% distinct
    negative_balances_by_month_key_cols = c('id','negative_dates')
    load_book_information(negative_balances_by_month_df,connection,negative_balances_by_month_table_name,db_schema,negative_balances_by_month_key_cols,analytics = T)


    withdrawals_sum_by_month_df = unnest_analytics_df(next_load_book_info,withdrawals_sum_by_month)
    withdrawals_sum_by_month_key_cols = c('id','date')
    load_book_information(withdrawals_sum_by_month_df,connection,withdrawals_sum_by_month_table_name,db_schema,withdrawals_sum_by_month_key_cols,analytics = T)

    daily_cash_flows_df = unnest_analytics_df(next_load_book_info,daily_cash_flows)
    daily_cash_flows_key_cols = c('id','date')
    load_book_information(daily_cash_flows_df,connection,daily_cash_flows_table_name,db_schema,daily_cash_flows_key_cols,analytics = T)

    deposits_sum_by_month_df = unnest_analytics_df(next_load_book_info,deposits_sum_by_month)
    deposits_sum_by_month_key_cols = c('id','date')
    load_book_information(deposits_sum_by_month_df,connection,deposits_sum_by_month_table_name,db_schema,deposits_sum_by_month_key_cols,analytics = T)


    estimated_revenue_by_month_df = unnest_analytics_df(next_load_book_info,estimated_revenue_by_month)
    estimated_revenue_by_month_key_cols = c('id','date')
    load_book_information(estimated_revenue_by_month_df,connection,estimated_revenue_by_month_table_name,db_schema,estimated_revenue_by_month_key_cols,analytics = T)


    daily_balances_df = unnest_analytics_df(next_load_book_info,daily_balances)
    daily_balances_key_cols = c('id','date')
    load_book_information(daily_balances_df,connection,daily_balances_table_name,db_schema,daily_balances_key_cols,analytics = T)


    completed_ids = next_load_book_info %>% unnest(data) %>% select(id)
    load_ids(completed_ids,connection,completed_table_name,db_schema)
    dbDisconnect(con)

  }

}
