import * as cdk from 'aws-cdk-lib';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import * as path from 'path';

//Globals added to props
interface MyStacksProps extends cdk.StackProps {
    projectName: string;
    env: {
      account: string;
      region: string;
        };
  }

export class APIStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: MyStacksProps) {
      super(scope, id, props);
  
      const { projectName } = props
      const tableName = cdk.Fn.importValue('tableName')
      const table = dynamodb.Table.fromTableName(this, 'StorageTable', tableName);

        // create a new Lambda function to query the DynamoDB table
        const queryTableFn = new lambda.Function(this, 'TableFunction', {
        functionName: 'dynamoScanFunction',
        runtime: lambda.Runtime.PYTHON_3_9,
        handler: 'api_app.handler',
        timeout:cdk.Duration.seconds(600),
        code: lambda.Code.fromAsset(path.join(__dirname, '../src/api_get')),
        environment: {
            TABLE_NAME: tableName
        }
        });

        // grant read permissions to the Lambda function
        table.grantReadData(queryTableFn);

// create a new API Gateway
const api = new apigw.RestApi(this, 'MyApi', {
  restApiName: 'awsDataScan',
  description: 'This api queries a dynamodb table and returns usage data'
});

// create an endpoint to scan the DynamoDB table
const scanResource = api.root.addResource('scan');
const scanLambdaIntegration = new apigw.LambdaIntegration(queryTableFn);
scanResource.addMethod('GET', scanLambdaIntegration)}};
