% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/database_backend.R
\name{load_ids}
\alias{load_ids}
\title{Load IDs
Load Ids to a database, this should be run after all the data for a given group of Books has been completed. This table will be used to check the loaded data against what is unloaded}
\usage{
load_ids(book_df, connection, table_name, db_schema)
}
\arguments{
\item{book_df}{df containing the book information, get_books function will give you this data}

\item{connection}{connection object initalized from get_sql_connection}

\item{table_name}{the name of the table you want to load the book data into}

\item{db_schema}{the schema where you want/have the table}
}
\description{
Load IDs
Load Ids to a database, this should be run after all the data for a given group of Books has been completed. This table will be used to check the loaded data against what is unloaded
}
