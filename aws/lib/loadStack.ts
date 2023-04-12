import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as path from 'path';


//Globals added to props
interface MyStacksProps extends cdk.StackProps {
    projectName: string;
    env: {
      account: string;
      region: string;
        };
  }

export class LoadStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: MyStacksProps) {
    super(scope, id, props);

    const { projectName, env } = props

    const bucketName = cdk.Fn.importValue('bucketName');
    const tableName = cdk.Fn.importValue('tableName')

        // create Lambda function using Docker image
    const myFunction = new lambda.DockerImageFunction(this, 'MyFunction', {
        code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, '../src/dynamo_put')),
        architecture: lambda.Architecture.X86_64,
        environment: {
            'REGION': env.region,
            'DYNAMO_TABLE_NAME': tableName
        },
        timeout: cdk.Duration.seconds(600),
        });

        // add a trigger to the S3 bucket to invoke the Lambda function when a CSV file is uploaded
        const bucket = s3.Bucket.fromBucketName(this, 'TargetBucket', bucketName);
        const table = dynamodb.Table.fromTableName(this, 'StorageTable', tableName);

        // add an event trigger to the bucket for the Lambda function on a PUT operation
        bucket.addEventNotification(s3.EventType.OBJECT_CREATED_PUT, new s3n.LambdaDestination(myFunction));

        //Adding additions for permissions and events
        bucket.grantRead(myFunction)
        table.grantWriteData(myFunction)
    }
      }

   