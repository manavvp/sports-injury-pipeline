provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      Project   = "sports-injury-pipeline"
      ManagedBy = "terraform"
    }
  }
}
