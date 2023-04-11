import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';


//Globals added to props
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

    
  
   // create DynamoDB table
   const myTable = new dynamodb.Table(this, `${projectName}DynamoTable`, {
    partitionKey: { name: 'identity/LineItemId', type: dynamodb.AttributeType.STRING },
    billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    removalPolicy: cdk.RemovalPolicy.DESTROY
  });

  

  new cdk.CfnOutput(this, `${projectName}BucketName`, {
    value: bucket.bucketName,
    exportName: 'bucketName'
  });

  new cdk.CfnOutput(this, `${projectName}TableName`, {
    value: myTable.tableName,
    exportName: 'tableName'
  });

  }
  
  }

