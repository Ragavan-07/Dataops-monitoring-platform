# Using Amazon S3 for storage

Do this only after the pipeline works locally. S3 storage for this project costs very little (a few GB at most), but AWS bills you for what you use, so delete the bucket when you are done.

## 1. Create an AWS account and turn on a budget alert

1. Sign up at https://aws.amazon.com.
2. In the console, search for **Billing and Cost Management → Budgets** and create a monthly budget of $5 with an email alert. This protects you from surprise bills.

## 2. Create a bucket

1. Open **S3 → Create bucket**.
2. Bucket name: must be globally unique, for example `dataops-events-yourname-2026`.
3. Region: pick one close to you (for example `us-east-1`) and remember it.
4. Leave **Block all public access** turned ON.
5. Click **Create bucket**.

## 3. Create an IAM user that can only use this bucket

Never use your root account keys.

1. Open **IAM → Policies → Create policy → JSON** and paste this, replacing `YOUR-BUCKET` twice:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
      "Resource": "arn:aws:s3:::YOUR-BUCKET"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::YOUR-BUCKET/*"
    }
  ]
}
```

2. Name it `dataops-pipeline-s3` and create it.
3. Open **IAM → Users → Create user**, name it `dataops-pipeline`, choose **Attach policies directly** and select `dataops-pipeline-s3`.
4. Open the new user → **Security credentials → Create access key** → choose **Application running outside AWS**. Copy the Access key ID and Secret access key. You will not see the secret again.

## 4. Point the project at S3

Edit `.env` (never commit this file):

```
STORAGE_MODE=s3
S3_BUCKET=dataops-events-yourname-2026
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
```

Restart so the containers pick up the new settings:

```bash
docker compose up -d
```

Trigger the DAG again. The first Spark run in S3 mode downloads the `hadoop-aws` library from Maven, so it takes an extra minute. In the S3 console you should now see `raw/`, `validated/`, `quarantine/` and `curated/` folders.

## 5. Clean up when you are done

Empty the bucket, delete it, and delete the IAM user's access key. If a key is ever pushed to GitHub by accident, delete it in IAM immediately and create a new one.
