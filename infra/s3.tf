resource "aws_s3_bucket" "hot_reload" {
  count         = data.aws_caller_identity.this.id == "000000000000" ? 1 : 0
  bucket        = "hot-reload"
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

resource "aws_s3_bucket" "this" {
  bucket        = format("%s-website-%s", var.aws_project, local.app_id)
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "this" {
  bucket = aws_s3_bucket.this.id
  acl    = "private"

  depends_on = [aws_s3_bucket_ownership_controls.this]
}

resource "aws_s3_bucket_website_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "index.html"
  }
}

# Data lake for the data engineering ETL job (Bronze/Silver/Gold, landing zone).
# Private: holds employee data, so it is not the CloudFront website bucket.
resource "aws_s3_bucket" "lake" {
  count         = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  bucket        = format("%s-lake-%s", var.aws_project, local.app_id)
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

resource "aws_s3_bucket_public_access_block" "lake" {
  count                   = length(aws_s3_bucket.lake)
  bucket                  = aws_s3_bucket.lake[count.index].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
