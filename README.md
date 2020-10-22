# ocrolusR
The ocrolusR package has simple functions for accessing and storing Ocrolus book data into a database. It was built with analysts who want to pull and archive the data from their APIs in mind.  You'll need to github install the package.

The main **ocrolousR** functions:
  * Get Book Data
  * Get Book Status
  * Load Book Data to DB while checking if it exists first
  * Checks Books that are pending and pulls their data when complete
  * Extracts Data about the books
  * Loads all data to a database.

If anyone wants to help build dummy json responses for testing, that would be a huge addition.  Right now, it's all been tested locally.

## Package Installation

``` devtools::install_github("atroiano/ocrolusR") ```
