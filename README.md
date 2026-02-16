# EMR Spark Jobs - Customer Analytics

Production PySpark jobs for EMR on EKS.

## Structure

```
emr-spark-jobs/
├── scripts/
│   └── customer_analytics.py          # Customer transaction analytics ETL
├── infrastructure/
│   └── job-config.yaml                # Job configuration parameters
├── cloudformation/
│   └── emr-job-deployment.yaml        # Deployment tracking stack
└── buildspec.yml                      # CodeBuild CI/CD pipeline
```

## CI/CD Flow

1. Push to `main` branch
2. CodeBuild triggered automatically
3. Script uploaded to S3 with commit hash: `customer_analytics_<hash>.py`
4. CloudFormation stack updated with deployment metadata
5. EMR on EKS job submitted automatically
6. Job logs sent to CloudWatch and S3

## Deployment Tracking

All deployments tracked in CloudFormation stack: `emr-customer-analytics-deployment`

Stack outputs include:
- Deployment version (commit hash)
- Deployment timestamp
- Script S3 path
- Commit author and message
- EMR job run ID

## Manual Job Submission

```bash
aws emr-containers start-job-run \
  --virtual-cluster-id zjvo6fcbjmep12ajvzcw48wn4 \
  --name customer-analytics-manual \
  --execution-role-arn arn:aws:iam::841306720465:role/EMRJobExecutionRole \
  --release-label emr-7.0.0-latest \
  --job-driver file://infrastructure/job-config.yaml
```

## Resources

- **Virtual Cluster:** `emr-demo-vc` (`zjvo6fcbjmep12ajvzcw48wn4`)
- **EKS Cluster:** `emr-demo-cluster`
- **Namespace:** `emr-spark-jobs`
- **S3 Bucket:** `emr-demo-841306720465`
- **Execution Role:** `EMRJobExecutionRole`
