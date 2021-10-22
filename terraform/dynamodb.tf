resource "aws_dynamodb_table" "python_etl_table" {
  name           = "python-etl-table"
  billing_mode   = "PROVISIONED"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "date"

  attribute {
    name = "date"
    type = "S"
  }

  tags = {
    name = "python-etl-table"
  }
}