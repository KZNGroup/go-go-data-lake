#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { GoGoDataLakeStack } from '../lib/go-go-data-lake-stack';

const app = new cdk.App();
new GoGoDataLakeStack(app, 'go-go-data-lake', {

});