#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { AthenaStack } from '../lib/projectStack';


const GLOBALS = {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT as string, region: 'eu-central-1'}
}

const app = new cdk.App();
const athenaStack = new AthenaStack(app, 'Aws2Stack', GLOBALS);