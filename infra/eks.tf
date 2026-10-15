resource "aws_eks_cluster" "this" {
  count    = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  name     = format("%s-%s", var.aws_project, local.app_id)
  version  = "1.36"
  role_arn = local.eks_role_arn

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  vpc_config {
    endpoint_public_access  = true
    endpoint_private_access = true
    subnet_ids              = local.public_subnet_ids
    security_group_ids      = data.aws_security_groups.this.ids
  }

  tags = local.app_tags
}

resource "aws_eks_node_group" "this" {
  count           = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  cluster_name    = one(aws_eks_cluster.this.*.name)
  node_group_name = format("%s-%s", var.aws_project, local.app_id)
  node_role_arn   = local.eks_role_arn
  subnet_ids      = local.public_subnet_ids
  capacity_type   = var.aws_eks_type
  instance_types  = ["t3.medium", "t3a.medium", "t2.medium"]
  disk_size       = 50

  scaling_config {
    desired_size = 1
    max_size     = 1
    min_size     = 1
  }

  update_config {
    max_unavailable = 1
  }

  tags = local.app_tags
}

# Data job pods use the node role's credentials; allow them the lake bucket only.
resource "aws_iam_role_policy" "eks_node_lake" {
  count = length(aws_s3_bucket.lake)
  name  = format("%s-lake-access-%s", var.aws_project, local.app_id)
  role  = element(split("/", local.eks_role_arn), 1)

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.lake[0].arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.lake[0].arn}/*"]
      }
    ]
  })
}

resource "null_resource" "helm_chart" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0

  triggers = {
    cluster_id  = one(aws_eks_node_group.this.*.id)
    source_hash = local.helm_hash
    aws_ds_ip   = var.aws_ds_ip
  }

  provisioner "local-exec" {
    command = <<-EOT
      if [ -z "${var.aws_ds_ip}" ] || [ "${var.aws_ds_ip}" = "null" ]; then echo "ERROR: aws_ds_ip is not set"; exit 1; fi && \
      aws eks update-kubeconfig --region ${data.aws_region.this.region} --name ${one(aws_eks_cluster.this.*.name)} || true && \
      helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ && helm repo update && \
      helm upgrade --cleanup-on-fail \
        --install jupyterhub jupyterhub/jupyterhub \
        --set proxy.secretToken="${sha256(random_pet.this.id)}" \
        --set hub.config.LDAPAuthenticator.server_address="${var.aws_ds_ip}" \
        --set-string "hub.config.LDAPAuthenticator.allowed_groups[0]=CN=${local.app_id}\,OU=Users\,OU=CORP\,DC=corp\,DC=codingworkshop\,DC=net" \
        --namespace default --values ./helm/config.yaml
    EOT
  }
}

# Uploads the sample source files to the lake's landing zone: LMS events for
# Bronze ingestion, and the HR CSVs + schema.sql for the --seed-source bootstrap.
resource "null_resource" "lake_landing" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0

  triggers = {
    bucket      = one(aws_s3_bucket.lake.*.bucket)
    source_hash = sha256(join("", [for f in fileset(local.sample_data_dir, "**") : filesha256(format("%s/%s", local.sample_data_dir, f))]))
  }

  provisioner "local-exec" {
    command = <<-EOT
      if [ ! -d "${local.sample_data_dir}" ]; then echo "WARN: ${local.sample_data_dir} not found, skipping landing upload"; exit 0; fi && \
      aws s3 sync "${local.sample_data_dir}/learning_activity" "s3://${one(aws_s3_bucket.lake.*.bucket)}/landing/learning_activity" --only-show-errors && \
      aws s3 sync "${local.sample_data_dir}" "s3://${one(aws_s3_bucket.lake.*.bucket)}/landing/hr" --exclude "*" --include "*.csv" --include "schema.sql" --only-show-errors
    EOT
  }
}

resource "null_resource" "helm_python_job" {
  for_each = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? local.data_names_python : {}

  triggers = {
    cluster_id  = one(aws_eks_node_group.this.*.id)
    source_hash = timestamp()
  }

  provisioner "local-exec" {
    environment = { for k, v in local.data_job_env : "JOB_ENV_${k}" => v }
    command     = <<-EOT
      helm upgrade --cleanup-on-fail \
        --install ${replace(each.key, "_", "-")}-python-job ./helm/ \
        --set pythonJob.enabled=true \
        --set pythonJob.hostNetwork=true \
        --set pythonJob.name=${replace(each.key, "_", "-")} \
        --set-string pythonJob.args="${try(each.value.args, "")}" \
        --set-file pythonJob.scriptContent=${each.value.path}/${each.value.file} \
        --set-file pythonJob.requirementsContent=${each.value.path}/${each.value.reqs} \
        ${join(" ", [for f in fileset(each.value.path, "*.py") : "--set-file pythonJob.modules.${trimsuffix(f, ".py")}=${each.value.path}/${f}" if f != each.value.file])} \
        ${local.helm_env_flags} \
        --namespace default
    EOT
  }

  depends_on = [null_resource.helm_chart, null_resource.lake_landing, aws_iam_role_policy.eks_node_lake]
}
