#' Get Data From the Book API
#'
#' This function calls the api.ocrolus.com/v1/books API.  string is optional and will allow you to
#' search for a specific book if passed
#' @param user username for the api
#' @param password password for the website
#' @param string Fragment of Book name to search for
#' @return
#' @export

get_books <- function(user,password, string = NULL){
  path <- "https://api.ocrolus.com/v1/books"
  request <- GET(url = path
                 ,authenticate(user, password, type = "basic")
                 ,query = list(search = string )
  )
  status = request$status_code

  if (status == 200){
    response <- content(request, as = "text", encoding = "UTF-8")
    response_json = fromJSON(response, flatten = TRUE)
    if (response_json$response %>% length == 0){
      warning('No Books Found')
      return(response_json)
    }
    df <- response_json %>%
      data.frame() %>% rename_all(~str_replace(.,'response.',''))
    return(df)
  } else {
    warning('Response Not 200 ')
    return(status)
  }
}

#' Get Data From the Book Status API
#'
#' This function calls the https://api.ocrolus.com/v1/book/status API.  string is optional and will allow you to
#' search for a specific book if the pk_string is passed
#' @param user username for the api
#' @param password password for the website
#' @param pk_string Book PK
#' @return response_json from api
#' @export
get_book_status = function(user,password,pk_string){
  path <- "https://api.ocrolus.com/v1/book/status"
  request <- GET(url = path
                 ,authenticate(user, password, type = "basic")
                 ,query = list(pk = pk_string )
  )
  response <- content(request, as = "text", encoding = "UTF-8")
  response_json = fromJSON(response, flatten = TRUE)
  return(response_json)
}


#' Get Data From the Book Info API
#'
#' This function calls the https://api.ocrolus.com/v1/book/info API.  pk_string will tell the API what book to return
#' @param user username for the api
#' @param password password for the website
#' @param pk_string Book PK
#' @return response_json from api
#' @export
get_book_info = function(user,password,pk_string){

  path <- "https://api.ocrolus.com/v1/book/info"
  request <- GET(url = path
                 ,authenticate(user, password, type = "basic")
                 ,query = list(pk = pk_string )
  )
  response <- content(request, as = "text", encoding = "UTF-8")
  response_json = fromJSON(response, flatten = TRUE)
  return(response_json)
}

#' Get Data From Transaction API
#'
#' This function calls thehttps://api.ocrolus.com/v1/transaction API.  book_pk is required
#' @param user username for the api
#' @param password password for the website
#' @param book_pk Book PK
#' @return response_json from api
#' @export
get_transactions = function(user,password,book_pk){

  path <- "https://api.ocrolus.com/v1/transaction"
  request <- GET(url = path
                 ,authenticate(user, password, type = "basic")
                 ,query = list(book_pk = book_pk )
  )
  response <- content(request, as = "text", encoding = "UTF-8")
  response_json = fromJSON(response, flatten = TRUE)
  return(response_json)
}


#' Get Data From The Analytics API
#'
#' This function calls the https://api.ocrolus.com/v1/book/summary API.  book_pk is required and is used to return the book data
#' @param user username for the api
#' @param password password for the website
#' @param book_pk Book PK
#' @return response_json from api
#' @export
get_analytics = function(user,password,book_pk){

  path <- "https://api.ocrolus.com/v1/book/summary"
  request <- GET(url = path
                 ,authenticate(user, password, type = "basic")
                 ,query = list(pk = book_pk )
  )
  response <- content(request, as = "text", encoding = "UTF-8")
  response_json = fromJSON(response, flatten = TRUE)
  return(response_json)
}

