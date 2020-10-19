#' Make the Analytics into a data frame
#' @param analyics_repsonse Reponse from the analytics API
#' @param pluck_value part of the response to grab and enframe
#' @return DF of analytics value you plucked
#' @export
enframe_monthly_analyics = function(analyics_repsonse,pluck_value){
  response = pluck_map(analyics_repsonse,pluck_value)
  response = response %>% enframe %>% unnest(value) %>% mutate(name = as.Date(as.POSIXct(str_c(name,'/01'), format = '%m/%Y/%d'))) %>% rename(month = name)
}

#' Get Specific Analytics Reponse from API
#' @param analyics_repsonse Reponse from the analytics API
#' @return Bank Accounts reponse from the analytics API
#' @export
get_bank_account_from_analyics = function(analyics_repsonse){
  map(analyics_repsonse,pluck('bank_accounts'))

}

#' Unnest Bank Transactions
#' @param analyics_repsonse Reponse from the analytics API
#' @return Bank Accounts reponse from the analytics API
#' @export
unnest_bank_transactions = function(df,col){
  df_unnested = df %>%    unnest({{col}})
  cols =colnames(df)
  if ('bbox' %in% cols){
    make_bbox_string(df_unnested)
  }

}

#' used to grab the min and max columns from the bank accounts
#' @param df df with the columns to grab
#' @param col_name the column containing the min and max values
#' @return df with the selected columns
#' @export
get_min_max_by_month = function(df,col_name){
  month_cols = df %>% select(contains(col_name)) %>% colnames

  df_data = df
  month_data = map(month_cols, function(x,df=df_data){
    df_unnested = df %>% select(x) %>% unnest(cols= !!as.symbol(x))
    cols =colnames(df)
    if ('bbox' %in% cols){

      df_unnested = make_bbox_string(df_unnested)
    }
    df_unnested
  })
  month_data = month_data %>% reduce(bind_rows)
  month_data_cols = colnames(month_data)
  if(nrow(month_data)>0){
    if('txn_date' %in% month_data_cols ){
      month_data  = month_data %>% mutate(
        txn_date = as.Date(as.POSIXct(txn_date, format = '%m/%d/%Y'))
      )
    }
  }
  month_data
}

#' Get Transactions That return a single value
#' @param df df with the columns to grab
#' @param col_name the column containing the single values
#' @return df with the selected columns
#' @export
get_txn_info_single_row = function(df,col){
  df %>% select(contains(col)) %>%
    pivot_longer(cols=contains(col)) %>% separate(name,c('key','date'),sep= '[.]',fill='left') %>% select(-key) %>%
    mutate(date = convert_to_date(date) )
}

#' Unnest the Analytics DFs
#' @param df df with the columns to grab
#' @param col the column to extract
#' @return df with the selected columns
#' @export

unnest_analytics_df= function(df,col)
{
  df %>% unnest(cols={{col}}) %>% select_if(~!is.list(.x))

}
#' Get Negative Value Dates
#' @param df df with the columns to grab
#' @param col the column to extract
#' @return df with the selected columns
#' @export
get_negative_balance <- function(df,col_name){
  month_cols = df %>% select(contains(col_name)) %>% colnames
  df_data = df
  month_data = map(month_cols, function(x,df=df_data){
    df_unnested = df %>% select(x) %>% pull(x) %>% compact
  })
  month_data = month_data %>% reduce(c) %>% enframe %>% unnest(value) %>% select(negative_dates = value)
}
