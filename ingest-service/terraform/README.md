# HPDS Ingest Terraform Infrastructure

Production-grade Terraform configuration for deploying HPDS Ingest Service on AWS EC2.

## Overview

This Terraform module provisions:
- EC2 instance (i7i family with local NVMe)
- IAM role with S3 access for data/configs
- Security group with SSH access
- CloudWatch Logs integration
- Auto-termination on completion/failure

## Prerequisites

```bash
# Required tools
terraform >= 1.5
aws-cli >= 2.0

# AWS credentials
aws configure
```

## Quick Start

```bash
# Initialize Terraform
terraform init

# Plan deployment
terraform plan -var="instance_type=i7i.24xlarge"

# Apply configuration
terraform apply -auto-approve

# Monitor instance
terraform output instance_id
aws logs tail /aws/ec2/hpds-ingest --follow

# Destroy (cleanup)
terraform destroy
```

## Configuration

### Required Variables

```hcl
variable "data_bucket" {
  description = "S3 bucket containing input parquet data"
  type        = string
}

variable "config_bucket" {
  description = "S3 bucket containing dataset configs (JSONL)"
  type        = string
}

variable "output_bucket" {
  description = "S3 bucket for output artifacts"
  type        = string
}
```

### Optional Variables

```hcl
variable "instance_type" {
  description = "EC2 instance type (i7i family recommended)"
  type        = string
  default     = "i7i.24xlarge"
}

variable "max_observations_per_file" {
  description = "Per-file observation limit"
  type        = number
  default     = 1000000
}

variable "max_observations_per_concept" {
  description = "Per-concept observation limit"
  type        = number
  default     = 100000000
}

variable "auto_terminate" {
  description = "Terminate instance on completion/failure"
  type        = bool
  default     = true
}

variable "ssh_key_name" {
  description = "EC2 key pair for SSH access"
  type        = string
  default     = null
}

variable "vpc_id" {
  description = "VPC ID for deployment"
  type        = string
  default     = null  # Uses default VPC if not specified
}

variable "subnet_id" {
  description = "Subnet ID for instance"
  type        = string
  default     = null  # Uses default subnet if not specified
}

variable "allowed_ssh_cidr" {
  description = "CIDR blocks allowed for SSH access"
  type        = list(string)
  default     = ["0.0.0.0/0"]  # Restrict in production!
}
```

### Example terraform.tfvars

```hcl
# terraform.tfvars
instance_type                = "i7i.24xlarge"
data_bucket                  = "my-hpds-data-bucket"
config_bucket                = "my-hpds-config-bucket"
output_bucket                = "my-hpds-output-bucket"
max_observations_per_file    = 1000000
max_observations_per_concept = 100000000
auto_terminate               = true
ssh_key_name                 = "my-ec2-key"
vpc_id                       = "vpc-12345678"
subnet_id                    = "subnet-87654321"
allowed_ssh_cidr             = ["10.0.0.0/8"]
```

## Architecture

### Resource Graph

```
┌─────────────────────────────────────────────────────┐
│ S3 Buckets                                          │
│ ├─ data_bucket (input parquet files)               │
│ ├─ config_bucket (dataset configs)                 │
│ └─ output_bucket (results)                         │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│ IAM Role & Instance Profile                        │
│ └─ Policy: S3 read/write permissions               │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│ EC2 Instance (i7i.24xlarge)                        │
│ ├─ User Data Script (entrypoint.sh)               │
│ ├─ Local NVMe: /mnt/nvme (spool directory)        │
│ ├─ Auto-configured JVM (heap + direct memory)     │
│ └─ Auto-termination on completion                 │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│ CloudWatch Logs                                     │
│ └─ Log Group: /aws/ec2/hpds-ingest                │
│    └─ Log Stream: {instance-id}/{timestamp}       │
└─────────────────────────────────────────────────────┘
```

### Instance Initialization Flow

1. **Instance Launch**
   - Terraform creates EC2 instance with user_data script
   - IAM role attached for S3 access
   - Security group allows SSH from allowed_ssh_cidr

2. **User Data Execution** (entrypoint.sh)
   ```bash
   # Auto-detect instance type
   INSTANCE_TYPE=$(ec2-metadata --instance-type | cut -d " " -f 2)

   # Calculate JVM settings
   TOTAL_RAM_GB=$(free -g | awk '/^Mem:/{print $2}')
   HEAP_SIZE_GB=$((TOTAL_RAM_GB * 80 / 100))
   DIRECT_MEMORY_GB=$((TOTAL_RAM_GB - HEAP_SIZE_GB - 10))

   # Mount NVMe for spool
   mkfs.ext4 /dev/nvme1n1
   mount /dev/nvme1n1 /mnt/nvme

   # Download configs from S3
   aws s3 sync s3://${config_bucket} /opt/data/configs

   # Run ingestion
   java -Xms${HEAP_SIZE_GB}g -Xmx${HEAP_SIZE_GB}g \
     -XX:MaxDirectMemorySize=${DIRECT_MEMORY_GB}g \
     -jar /opt/hpds/ingest-service.jar

   # Upload results
   aws s3 sync /opt/data/output s3://${output_bucket}

   # Terminate (if auto_terminate=true)
   shutdown -h now
   ```

3. **Monitoring**
   - Logs streamed to CloudWatch Logs
   - Instance status visible in EC2 console
   - Terraform outputs provide instance ID and public IP

## Instance Types

### Recommended: i7i Family

| Instance | vCPU | RAM | NVMe | Network | Use Case |
|----------|------|-----|------|---------|----------|
| i7i.24xlarge | 96 | 768 GB | 4 × 3.75 TB | 50 Gbps | Production (3k+ patients) |
| i7i.16xlarge | 64 | 512 GB | 4 × 2.5 TB | 37.5 Gbps | Staging |
| i7i.12xlarge | 48 | 384 GB | 4 × 1.875 TB | 28.125 Gbps | Dev |
| i7i.8xlarge | 32 | 256 GB | 2 × 3.75 TB | 18.75 Gbps | Testing |
| i7i.4xlarge | 16 | 128 GB | 1 × 3.75 TB | 10 Gbps | Smoke tests |

**Why i7i?**
- High memory-to-vCPU ratio (8 GB per vCPU)
- Local NVMe SSD for spool performance
- Optimized for data-intensive workloads
- Available in all major AWS regions

### Alternative: r7i Family (No Local Storage)

If NVMe not required (small datasets with minimal spooling):

| Instance | vCPU | RAM | Use Case |
|----------|------|-----|----------|
| r7i.24xlarge | 96 | 768 GB | Production (small datasets) |
| r7i.16xlarge | 64 | 512 GB | Staging |

**Note**: Requires EBS volume for spool directory

## Cost Optimization

### Strategies

**1. Use Spot Instances** (60-70% savings)

```hcl
# main.tf
resource "aws_instance" "hpds_ingest" {
  instance_market_options {
    market_type = "spot"
    spot_options {
      max_price          = "4.00"  # i7i.24xlarge on-demand = $7.488/hr
      spot_instance_type = "one-time"
    }
  }
}
```

**2. Right-Size for Dataset**

| Dataset Size | Recommended Instance | Est. Duration | Est. Cost |
|--------------|---------------------|---------------|-----------|
| <1,000 patients | i7i.8xlarge | 3-4 hours | $10 |
| 1,000-2,000 patients | i7i.12xlarge | 3-5 hours | $14-19 |
| 2,000-4,000 patients | i7i.24xlarge | 2-3 hours | $15-23 |
| 4,000+ patients | i7i.24xlarge | 3-6 hours | $23-45 |

**3. Schedule During Off-Peak**

```hcl
# Use AWS Lambda to start instance during off-peak hours
# Reduces costs in some regions
```

**4. Use Reserved Instances** (for frequent ingestion)

- 1-year commitment: 30% savings
- 3-year commitment: 50% savings

### Cost Examples

**Scenario: RECOVER Adult (18 datasets, 3,443 patients)**

| Strategy | Instance | Duration | Cost |
|----------|----------|----------|------|
| On-demand | i7i.24xlarge | 3 hours | $22.50 |
| Spot | i7i.24xlarge | 3 hours | $8.50 |
| On-demand (right-sized) | i7i.12xlarge | 5 hours | $18.70 |
| Spot (right-sized) | i7i.12xlarge | 5 hours | $7.00 |

**Recommendation**: Use spot i7i.24xlarge for production ($8.50 per run)

## Monitoring & Debugging

### Get Instance Details

```bash
# Get instance ID
terraform output instance_id

# Get public IP (if SSH enabled)
terraform output public_ip

# Get instance state
aws ec2 describe-instances --instance-ids $(terraform output -raw instance_id)
```

### View Logs

```bash
# Tail logs in real-time
aws logs tail /aws/ec2/hpds-ingest --follow

# Filter for errors
aws logs filter-log-events \
  --log-group-name /aws/ec2/hpds-ingest \
  --filter-pattern "ERROR"

# Get logs from specific time range
aws logs filter-log-events \
  --log-group-name /aws/ec2/hpds-ingest \
  --start-time $(date -u -d '1 hour ago' +%s)000 \
  --end-time $(date -u +%s)000
```

### SSH to Instance

```bash
# SSH (if ssh_key_name configured)
ssh -i ~/.ssh/my-ec2-key.pem ec2-user@$(terraform output -raw public_ip)

# Check process
ps aux | grep java

# View local logs
tail -f /opt/data/output/ingest.log

# Check disk usage
df -h
du -sh /mnt/nvme/spool
```

### Debug Failures

```bash
# Check user data execution log
ssh ec2-user@<instance-ip>
sudo cat /var/log/cloud-init-output.log

# Check for OOM kills
dmesg | grep -i "out of memory"

# Download heap dump (if OOM occurred)
aws s3 cp s3://${output_bucket}/hpds-oom.hprof ./
```

## Security

### IAM Role Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${data_bucket}/*",
        "arn:aws:s3:::${config_bucket}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl"
      ],
      "Resource": "arn:aws:s3:::${output_bucket}/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/ec2/hpds-ingest:*"
    }
  ]
}
```

### Security Group

```hcl
resource "aws_security_group" "hpds_ingest" {
  name_prefix = "hpds-ingest-"
  vpc_id      = var.vpc_id

  # SSH (optional, for debugging)
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_ssh_cidr
  }

  # Outbound (S3, CloudWatch)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

**Production recommendations:**
- Restrict SSH to VPN/bastion CIDR only
- Use Session Manager instead of SSH (no open ports)
- Enable VPC Flow Logs for network monitoring
- Use S3 bucket policies to restrict access

## Advanced Configuration

### Custom User Data Script

```hcl
# main.tf
data "template_file" "user_data" {
  template = file("${path.module}/custom-entrypoint.sh")

  vars = {
    heap_size_gb     = local.heap_size_gb
    direct_memory_gb = local.direct_memory_gb
    data_bucket      = var.data_bucket
    output_bucket    = var.output_bucket
  }
}

resource "aws_instance" "hpds_ingest" {
  user_data = data.template_file.user_data.rendered
}
```

### Multiple Environments

```hcl
# terraform.tfvars.prod
instance_type = "i7i.24xlarge"
data_bucket   = "prod-hpds-data"
output_bucket = "prod-hpds-output"
auto_terminate = true

# terraform.tfvars.dev
instance_type = "i7i.8xlarge"
data_bucket   = "dev-hpds-data"
output_bucket = "dev-hpds-output"
auto_terminate = false  # Keep running for debugging
```

Deploy with:
```bash
terraform apply -var-file=terraform.tfvars.prod
```

### Remote State

```hcl
# backend.tf
terraform {
  backend "s3" {
    bucket         = "my-terraform-state"
    key            = "hpds-ingest/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-locks"
  }
}
```

### Workspace-Based Environments

```bash
# Create workspaces
terraform workspace new prod
terraform workspace new dev

# Switch workspace
terraform workspace select prod
terraform apply

terraform workspace select dev
terraform apply
```

## Outputs

```hcl
output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.hpds_ingest.id
}

output "public_ip" {
  description = "Public IP address"
  value       = aws_instance.hpds_ingest.public_ip
}

output "instance_type" {
  description = "Instance type used"
  value       = aws_instance.hpds_ingest.instance_type
}

output "log_group" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.hpds_ingest.name
}

output "output_bucket" {
  description = "S3 bucket with results"
  value       = var.output_bucket
}
```

## Troubleshooting

### Terraform Errors

**Error**: `Error launching instance: InvalidParameterValue`

**Solution**: Check instance type availability in region
```bash
aws ec2 describe-instance-type-offerings \
  --filters "Name=instance-type,Values=i7i.24xlarge" \
  --region us-east-1
```

**Error**: `UnauthorizedOperation: You are not authorized to perform this operation`

**Solution**: Check IAM permissions for EC2/IAM operations
```bash
aws iam get-user
```

### Instance Failures

**Issue**: Instance terminates immediately

**Debug**: Check user data logs
```bash
aws ec2 get-console-output --instance-id $(terraform output -raw instance_id)
```

**Issue**: No logs in CloudWatch

**Debug**: Check IAM role has CloudWatch Logs permissions
```bash
aws logs describe-log-streams --log-group-name /aws/ec2/hpds-ingest
```

## Next Steps

- **Configuration**: See [Configuration Guide](../docs/CONFIGURATION.md) for application settings
- **Deployment**: See [Deployment Guide](../docs/DEPLOYMENT.md) for JVM tuning
- **Operations**: See [Operations Guide](../docs/OPERATIONS.md) for monitoring
