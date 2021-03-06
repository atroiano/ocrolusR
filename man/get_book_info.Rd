% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/api_data.R
\name{get_book_info}
\alias{get_book_info}
\title{Get Data From the Book Info API}
\usage{
get_book_info(user, password, pk_string)
}
\arguments{
\item{user}{username for the api}

\item{password}{password for the website}

\item{pk_string}{Book PK}
}
\value{
response_json from api
}
\description{
This function calls the https://api.ocrolus.com/v1/book/info API.  pk_string will tell the API what book to return
}
