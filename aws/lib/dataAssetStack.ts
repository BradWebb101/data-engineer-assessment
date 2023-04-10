import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as path from 'path';

interface MyStacksProps extends cdk.StackProps {
  projectName: string;
  env: {
    account: string;
    region: string;
      };

}

export class DataAssetStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: MyStacksProps) {
    super(scope, id, props);

    const { projectName } = props

    // Create an S3 bucket with public block access and versioning enabled
    const bucket = new s3.Bucket(this, `${projectName}Bucket`, {
      bucketName: `${projectName}storagebucket`.toLowerCase(),
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: true
    });

  // Upload CSV file to the S3 bucket
  new s3deploy.BucketDeployment(this, `${projectName}CsvDeployment`, {
    sources: [s3deploy.Source.asset(path.join(__dirname, '../../assets/data'))],
    destinationBucket: bucket,
    destinationKeyPrefix: 'data/',
  });
  
  }
}
