resource "aws_glue_catalog_database" "glue_raw" {
  name        = "sports_injury_raw"
  description = "Raw layer - sports injury and performance data from nflverse and Transfermarkt"
}

resource "aws_glue_catalog_database" "glue_processed" {
  name        = "sports_injury_processed"
  description = "Processed (Silver) Layer - Sports Injury data from NFL and Football"
}