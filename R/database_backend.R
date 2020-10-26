#' Create Database Backend
#' Initalize a database backend
#' @param user_name username for the api
#' @param password password for the website
#' @param driver_name Name of the driver being used to ODBC to the DB
#' @param server Server IP or Name
#' @param database Database Name
#' @param port Port of the DB
#' @return connection object
#' @export
get_sql_connection <- function(user_name, password, driver_name, server, database, port){
  DBI::dbConnect(odbc::odbc(),
                 Driver   = driver_name,
                 Server   = server,
                 Database = database,
                 UID      = user_name,
                 PWD      = password,
                 Port     = port)
}

#' Create Database Backend
#' Initalize a database backend
#' @param connection connection object
#' @param table_name the name of the table to get data from
#' @param schema the schema the table is located in
#' @return connection object
#' @export
get_all_data_from_table = function(connection,table_name,schema){
  query = dbGetQuery(connection,str_c("select * from ", schema,'.',table_name))
  as_tibble(query)
}

#' Load Book Data
#' Load data to a database, designed use is from the get_books function. This checks if a table is created and will compare the loaded data against what was pulled from the API
#' @param book_df df containing the book information, get_books function will give you this data
#' @param connection connection object initalized from get_sql_connection
#' @param table_name the name of the table you want to load the book data into
#' @param db_schema the schema where you want/have the table
#' @param return_unloaded_books_df return the dataframe with the unloaded books, T or F
#' @export

load_book_data <- function(book_df,connection,table_name,db_schema,return_unloaded_books_df = F){
  table_found = check_table_in_db(connection,table_name)
  append = F
  overwrite = T
  books_to_load = book_df
  if (table_found){
    append = T
    overwrite = F
    loaded_data = get_all_data_from_table(connection,table_name,db_schema)
    loaded_data = loaded_data %>% select(pk,id) %>% mutate(loaded = 1)
    books_to_load = books_to_load %>% left_join(loaded_data)
    books_to_load = filter(books_to_load,is.na(loaded)) %>% select(-loaded)
  }
  if (nrow(books_to_load)>0){
    DBI::dbWriteTable(con = connection,name = DBI::Id(schema = db_schema,table=table_name), books_to_load ,overwrite = overwrite,append=append)
  }
  if (return_unloaded_books_df) return(books_to_load)

}


#' Load Book Status
#' Load the status of the books into the database, designed use is from the book_status function. This checks if a table is created and will compare the loaded data against what was pulled from the API
#' @param book_df df containing the book information, get_books function will give you this data
#' @param connection connection object initalized from get_sql_connection
#' @param table_name the name of the table you want to load the book data into
#' @param db_schema the schema where you want/have the table
#' @export
load_book_status_data <- function(book_df,connection,table_name,db_schema,return_unloaded_books_df = F){
  table_found = check_table_in_db(connection,table_name)
  append = F
  overwrite = T
  books_to_load = book_df %>% select(-error,-type_error,-book_status)
  if (table_found){
    append = T
    overwrite = F
    loaded_data = get_all_data_from_table(connection,table_name,db_schema)
    loaded_data = loaded_data %>% select(pk,bk_pk,bk_id,bk_docs_pk) %>% mutate(loaded = 1)
    books_to_load = books_to_load %>% left_join(loaded_data)
    books_to_load = filter(books_to_load,is.na(loaded))%>% select(-loaded)
  }
  if (nrow(books_to_load)>0){
    DBI::dbWriteTable(con = connection,name = DBI::Id(schema = db_schema,table=table_name), books_to_load,overwrite = overwrite,append=append)
  }
  if (return_unloaded_books_df) return(books_to_load)

}

#' Check for Table in DB
#' Check if a table exists in the DB
#' @param connection connection object initalized from get_sql_connection
#' @param table_name the name of the table you want to load the book data into
#' @export
check_table_in_db <- function(connection,table_name){
  tables = db_list_tables(connection)
  return(table_name %in% tables)
}


#' Load Info to DB
#' Generic loader function for the analytics part of the Ocrolus API.  This will take a string of key columns to use a check and use that to validate there are not duplicates
#' @param book_df df containing the book information, get_books function will give you this data
#' @param connection connection object initalized from get_sql_connection
#' @param table_name the name of the table you want to load the book data into
#' @param db_schema the schema where you want/have the table
#' @param key_cols key columns for the section
#' @export
load_book_information <- function(data_df,connection,table_name,db_schema,key_cols,analytics = F){
  table_found = check_table_in_db(connection,table_name)
  append = F
  overwrite = T
  data_to_load = data_df
  if (analytics == T){
    if(ncol(data_to_load) ==1) {
      return()
    }
  }
  if (table_found){
    print('table_found')
    append = T
    overwrite = F
    loaded_data = get_all_data_from_table(connection,table_name,db_schema)
    loaded_data = loaded_data %>% select(all_of(key_cols)) %>% mutate(loaded = 1)
    data_to_load = data_to_load %>% left_join(loaded_data)
    data_to_load = filter(books_to_load,is.na(loaded))%>% select(-loaded)
  }
  if (nrow(data_to_load)>0){
    DBI::dbWriteTable(con = connection,name = DBI::Id(schema = db_schema,table=table_name), data_to_load,overwrite = overwrite,append=append)
  }

}


#' Load IDs
#' Load Ids to a database, this should be run after all the data for a given group of Books has been completed. This table will be used to check the loaded data against what is unloaded
#' @param book_df df containing the book information, get_books function will give you this data
#' @param connection connection object initalized from get_sql_connection
#' @param table_name the name of the table you want to load the book data into
#' @param db_schema the schema where you want/have the table
#' @export

load_ids <- function(book_df,connection,table_name,db_schema){
  table_found = check_table_in_db(connection,table_name)
  append = F
  overwrite = T
  books_to_load = book_df
  if (table_found){
    append = T
    overwrite = F
    loaded_data = get_all_data_from_table(connection,table_name,db_schema)
    loaded_data = loaded_data %>% mutate(loaded = 1)
    books_to_load = books_to_load %>% left_join(loaded_data)
    books_to_load = filter(books_to_load,is.na(loaded)) %>% select(-loaded)
  }
  if (nrow(books_to_load)>0){
    DBI::dbWriteTable(con = connection,name = DBI::Id(schema = db_schema,table=table_name), books_to_load ,overwrite = overwrite,append=append)
  }

}

