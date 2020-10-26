
get_new_books <- function(connection,
                          db_schema,
                          api_un,
                          api_pw,
                          books_table_name = 'OcrolusBooks',
                          book_verification_complete_table_name = 'OcrolusBooksVerificationComplete',
                          book_verifying_table_name = 'OcrolusBooksVerifying',
                          rejected_books_table_name  = 'OcrolusBooksRejected'
){



  books = get_books(user,password)
  new_books = load_book_data(books,connection,books_table_name,db_schema)
  rm(books)

  df_status = new_books  %>%
    group_by(id) %>%
    nest() %>%
    mutate(
      book_status = map(data, ~get_book_status(user,password,.$pk))
    )



  # Flow
  # get books
  # check for books that are loaded
  # poll all open books
  # when found grab and load the data
  # repeat
  #

  df_status = df_status %>%
    mutate(
      book_df = map(book_status,purrr::safely(get_docs_df))
      ,error = map(book_status, ~pluck(.,'error'))
      ,book_df = map(book_df, ~pluck(.,'result') )
      ,type_error = map(error,typeof)
    )

  df_status_book = df_status %>%
    filter(!is.na(type_error)) %>%
    unnest(cols=c(data,book_df))

  df_status_book_complete = df_status_book %>% filter(bk_docs_status == 'VERIFICATION_COMPLETE')

  newly_completed_books = load_book_status_data(df_status_book_complete,connection,book_verification_complete_table_name,db_schema)

  # str_c('DELETE FROM '
  # ,db_schema,'.',book_verifying_table_name, ' WHERE BK_PK in ( SELECT BK_PK FROM ',db_schema, '.',book_verification_complete_table_name)


  df_status_book_not_complete = df_status_book %>% filter(bk_docs_status != 'VERIFICATION_COMPLETE')

  df_status_book_rejected = df_status_book %>% filter(bk_docs_status == 'REJECTED')
  load_book_status_data(df_status_book_rejected,connection,rejected_books_table_name,db_schema)

  df_status_book_verifying = df_status_book %>% filter(bk_docs_status == 'VERIFYING')
  load_book_status_data(df_status_book_verifying,connection,book_verifying_table_name,db_schema)

  df_status_book_no_status = df_status_book %>% filter(is.na(bk_docs_status))
  load_book_status_data(df_status_book_no_status,connection,books_no_status_table_name,db_schema)


  ## Todo Do something with errors
  df_status_book_error = df_status_book %>% filter(!is.na(error))
  table_name = 'OcrolusBooksError'
  load_book_status_data(df_status_book_error,connection,table_name,db_schema)


}
