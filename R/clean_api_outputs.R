#' Create a DF from the Book Status data returned from the API
#' @param response_json the response json from the book_status api call
#' @return df with cleaned names and renamed
#' @export
get_docs_df = function(response_json){
  df <- response_json %>%  data.frame() %>% rename_all(~str_replace(.,'response.','')) %>% clean_names() %>% rename_all(~str_c('bk_',.))
}

#' Internal function used to pluck data from a list column DF
#' @param x list data
#' @param name  name you are trying to pluck
#' @return dataset
pluck_map = function(x,name){
  pluck(x,name,.default = NA)
}

#' Extract Details From the Bank Account JSON
#' @param book_info_response The response from the get_book_info api call
#' @return list of core_info, activity_info, activing_info_missing, periods
#' @export
extract_bank_accounts_details = function(book_info_response){
  enframed = book_info_response$response$bank_accounts %>% enframe
  core_info = enframed %>% mutate(
    holder_state = map_chr(value, ~pluck_map(.x,'holder_state'))
    ,bank_name = map_chr(value, ~pluck_map(.x,'bank_name'))
    ,holder_address_1 = map_chr(value, ~pluck_map(.x,'holder_address_1'))
    ,account_type = map_chr(value, ~pluck_map(.x,'account_type'))
    ,account_name = map_chr(value, ~pluck_map(.x,'name'))
    ,holder_country = map_chr(value, ~pluck_map(.x,'holder_country'))
    ,account_category = map_chr(value, ~pluck_map(.x,'account_category'))
    ,account_holder = map_chr(value, ~pluck_map(.x,'account_holder'))
    ,account_number = map_chr(value, ~pluck_map(.x,'account_number'))
    ,book_pk = map_chr(value, ~pluck_map(.x,'book_pk'))
    ,holder_address_2 = map_chr(value, ~pluck_map(.x,'holder_address_2'))
    ,pk = map_chr(value, ~pluck_map(.x,'pk'))
    ,bk_id = map_chr(value, ~pluck_map(.x,'id'))
    ,holder_zip = map_chr(value, ~pluck_map(.x,'holder_zip'))

  )
  activity_info_active= enframed %>% mutate(
    activity_info = map(value, ~pluck_map(.x,'activity_info'))
    ,active = map(activity_info, ~pluck_map(.x,'active'))
    ,missing = map(activity_info, ~pluck_map(.x,'missing'))
  ) %>% unnest(cols=c(active))

  activity_info_missing = enframed %>% mutate(
    activity_info = map(value, ~pluck_map(.x,'activity_info'))
    ,active = map(activity_info, ~pluck_map(.x,'active'))
    ,missing = map(activity_info, ~pluck_map(.x,'missing'))
  ) %>% unnest(cols=c(missing))

  activity_info_missing = enframed %>% mutate(
    activity_info = map(value, ~pluck_map(.x,'activity_info'))
    ,active = map(activity_info, ~pluck_map(.x,'active'))
    ,missing = map(activity_info, ~pluck_map(.x,'missing'))
  ) %>% unnest(cols=c(missing))

  periods = enframed %>% mutate(
    periods = map(value, ~pluck_map(.x,'periods'))
  ) %>% unnest(cols=c(periods))


  return(list(core_info = core_info,activity_info_active = activity_info_active,activity_info_missing =activity_info_missing, periods = periods))
}

#' Extract Details From the Bank Account JSON
#' @param df DF containing the data to extract
#' @param col_with_data column containing the data you want to extract
#' @param pluck_value value
#' @return list of core_info, activity_info, activing_info_missing, periods
extract_nested_df = function(df,col_with_data,pluck_value)
{
  df %>%
    mutate(
      extract = map({{col_with_data}},~pluck_map(.x,pluck_value))
    ) %>% unnest(extract) %>% select_if(~!(is.list(.x))) %>% clean_names()

}

#' Make bbox a string value
#' @param df DF containing the bbox column
#' @return df with bbox as a string
#' @export

make_bbox_string = function(df){
  df %>% mutate(
    bbox = map_chr(bbox,function(x){str_c(x,collapse = ',')})
  )
}

#' Pluck In the Mapping Function
#' @param x is value to map
#' @param name is the value to pluck
#' @return value plucked from list or NA if it wasn't found
#' @export
pluck_map_df = function(x,name){
  map_df(pluck(x,name,.default = NA),function(x){x})
}

#' Convert non standard date formats to standardized dates
#' @param col date col name
#' @return formated field
#' @export
convert_to_date = function(col){
  if(length(str_split(col,pattern = '/')[[1]]) == 2){
    return(as.Date(as.POSIXct(str_c(col,'/01'), format = '%m/%Y/%d')))
  }
  return(as.Date(as.POSIXct(col, format = '%m/%d/%Y')))
}
