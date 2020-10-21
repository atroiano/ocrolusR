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
#' @return connection object
#' @export
get_all_data_from_table = function(connection,table_name){
  query = dbGetQuery(connection,str_c("select * from ", table_name))
  as_tibble(query)
}
