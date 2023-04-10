import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

interface MyStacksProps extends cdk.StackProps {
  projectName: string;
env: {
  account: string;
  region: string;
};
datasetEndPoint: string;
}

export class QueryStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: MyStacksProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    // example resource
    // const queue = new sqs.Queue(this, 'AwsQueue', {
    //   visibilityTimeout: cdk.Duration.seconds(300)
    // });
  }
}
