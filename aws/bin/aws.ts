#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { DataAssetStack } from '../lib/dataAssetStack';
// import { LoadStack } from '../lib/loadStack'

const app = new cdk.App();

const GLOBALS = {
  projectName: 'DutchNlPipeline',
  env: { account: process.env.CDK_DEFAULT_ACCOUNT as string, region: process.env.CDK_DEFAULT_REGION as string }
}

const extractStack = new DataAssetStack(app, `${GLOBALS.projectName}ExtractStack`, GLOBALS);

// const loadStack = new LoadStack(app, `${GLOBALS.projectName}LoadStack`, GLOBALS);

// const queryStack 

// const visualStack 