terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.cluster.token
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_name
}

module "vpc" {
  source = "./modules/vpc"
  
  environment = var.environment
  vpc_cidr    = var.vpc_cidr
  
  tags = var.common_tags
}

module "eks" {
  source = "./modules/eks"
  
  environment         = var.environment
  cluster_name        = "${var.environment}-fraud-detection"
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  public_subnet_ids  = module.vpc.public_subnet_ids
  
  tags = var.common_tags
}

module "kafka" {
  source = "./modules/kafka"
  
  environment = var.environment
  namespace   = "kafka"
  
  depends_on = [module.eks]
}

module "cassandra" {
  source = "./modules/cassandra"
  
  environment = var.environment
  namespace   = "cassandra"
  
  depends_on = [module.eks]
}

module "monitoring" {
  source = "./modules/monitoring"
  
  environment = var.environment
  namespace   = "monitoring"
  
  depends_on = [module.eks]
}

resource "aws_s3_bucket" "ml_models" {
  bucket = "${var.environment}-fraud-detection-ml-models"
  
  tags = merge(var.common_tags, {
    Name = "${var.environment}-ml-models"
  })
}

resource "aws_s3_bucket_versioning" "ml_models" {
  bucket = aws_s3_bucket.ml_models.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ml_models" {
  bucket = aws_s3_bucket.ml_models.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
