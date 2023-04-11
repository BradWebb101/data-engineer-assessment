#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { DataAssetStack } from '../lib/dataAssetStack';
import { LoadStack } from '../lib/loadStack'


const app = new cdk.App();

const GLOBALS = {
  projectName: 'PostNlPipeline',
  env: { account: process.env.CDK_DEFAULT_ACCOUNT as string, region: 'eu-central-1' }
}

const dataAssetStack = new DataAssetStack(app, `${GLOBALS.projectName}ExtractStack`, GLOBALS);

const loadStack = new LoadStack(app, `${GLOBALS.projectName}LoadStack`, GLOBALS);

// const queryStack 

// const visualStack 