module "lambda_function" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "python-etl"
  description   = "Gathers covid data, manipulates it, and exports it."
  handler       = "index.lambda_handler"
  runtime       = "python3.8"

  source_path = "../src/lambda-python-etl"

  tags = {
    Name = "python-etl"
  }
}