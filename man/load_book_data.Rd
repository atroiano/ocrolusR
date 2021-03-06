% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/database_backend.R
\name{load_book_data}
\alias{load_book_data}
\title{Load Book Data
Load data to a database, designed use is from the get_books function. This checks if a table is created and will compare the loaded data against what was pulled from the API}
\usage{
load_book_data(
  book_df,
  connection,
  table_name,
  db_schema,
  return_unloaded_books_df = F
)
}
\arguments{
\item{book_df}{df containing the book information, get_books function will give you this data}

\item{connection}{connection object initalized from get_sql_connection}

\item{table_name}{the name of the table you want to load the book data into}

\item{db_schema}{the schema where you want/have the table}

\item{return_unloaded_books_df}{return the dataframe with the unloaded books, T or F}
}
\description{
Load Book Data
Load data to a database, designed use is from the get_books function. This checks if a table is created and will compare the loaded data against what was pulled from the API
}
