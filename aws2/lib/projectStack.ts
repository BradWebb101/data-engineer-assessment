import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as lambda from 'aws-cdk-lib/aws-lambda'
import * as s3n from 'aws-cdk-lib/aws-s3-notifications'
import * as path from 'path'
import * as ecr from 'aws-cdk-lib/aws-ecr'

//Globals added to props
interface MyStacksProps extends cdk.StackProps {
  env: {
    account: string;
    region: string;
      };
}

export class AthenaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: MyStacksProps) {
    super(scope, id, props);

    const { env } = props;

    // Create an S3 bucket to store the input CSV file
    const uploadBucket = new s3.Bucket(this, 'MyUploadBucket', {
      versioned: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

    const databaseBucket = new s3.Bucket(this, 'MyStorageBucket', {
      versioned: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

      // //Paths to add data, scripts,temp-dir
      // const dynamoPutLambda = new lambda.Function(this, 'MyLambdaFunction', {
      //   runtime: lambda.Runtime.PYTHON_3_9,
      //   code:lambda.Code.fromEcrImage(ecr.Repository.fromRepositoryName(this,'MyEcrRepository','postnlimage')),
      //   handler: 'app.handler',
      //   //I am getting an error pushing ducker images with this method. Manually pushed docker image and referenced above
      //   //code: lambda.Code.fromDockerBuild(path.join(__dirname, '../', 'src','etl_process')),
      //   //architecture: lambda.Architecture.X86_64,
      //   timeout: cdk.Duration.minutes(5)
      //   });

    const dynamoPutLambda = new lambda.Function(this, 'MyLambdaFunction', {
      runtime: lambda.Runtime.FROM_IMAGE,
      code:lambda.Code.fromEcrImage(ecr.Repository.fromRepositoryName(this, 'MyECRRepository', 'postnlpipeline')),
      //docker command to build this image docker build --platform linux/amd64 -t postnlpipeline .
      handler: lambda.Handler.FROM_IMAGE,
      timeout: cdk.Duration.minutes(5),
      environment: {
        DATABASE_BUCKET:databaseBucket.bucketName,
        PRIMARY_KEY:'identity/LineItemId',
        TIME_KEY:'lineItem/UsageEndDate',
        ACCOUNT_KEY:'lineItem/UsageAccountId'
        }
      });

    uploadBucket.grantReadWrite(dynamoPutLambda);
    databaseBucket.grantReadWrite(dynamoPutLambda);

    // Add an S3 object creation event notification to trigger the Lambda function
    uploadBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3n.LambdaDestination(dynamoPutLambda));


    const database = new glue.CfnDatabase(this, 'MyDatabase', {
      catalogId: env.account,
      databaseInput: {
        name: 'aws_cost_database',
        description: 'A database for all the costs of the AWS organisation across multiple accounts'
      }
    });

    const identitytable = new glue.CfnTable(this, 'identityTable',{ 
      catalogId:env.account,
      databaseName: database.ref,
            tableInput: {name: 'aws-costs-identity',
      description: 'Athena table for storing identity data for AWS costs',
      
          parameters: {
                  'serialization.format': '1',
                'compressionType': 'snappy', // specify the compression codec as Snappy
                'delimiter': ',',
                'skip.header.line.count': '1'
              },
              storageDescriptor: {
                columns: [
      {name: 'LineItemId', type: 'string'}, 
      {name: 'TimeInterval', type: 'string'}, 
      ],
      location: `s3://${uploadBucket.bucketName}/identity`, 
      inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
      
                outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
      
                serdeInfo: {
      
                  serializationLibrary: 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe', // specify the serde as LazyBinaryColumnarSerDe
      
                  parameters: {
      
                    'serialization.format': '1',
      
                    'compressionType': 'snappy' // specify the compression codec as Snappy
      
                  }
      
                }
      
              },
      
              partitionKeys: [
      { name: 'account', type: 'string' },
      { name: 'year', type: 'int' },
      { name: 'month', type: 'int' },
      { name: 'day', type: 'string' },
      ],
      tableType: 'EXTERNAL_TABLE'
      
            }
          });
      

      }
    }