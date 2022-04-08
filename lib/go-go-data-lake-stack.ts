import { Duration, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';

export class GoGoDataLakeStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const bucket = new s3.Bucket(this, 'go-go-bucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      publicReadAccess: false
    });

    const convertLambda = new lambda.Function(this, 'go-convert', {
      code: lambda.Code.fromAsset('src/convert'),
      handler: 'convert.main',
      runtime: lambda.Runtime.GO_1_X,
      memorySize: 512,
      timeout: Duration.minutes(2),
      architecture: lambda.Architecture.X86_64 // ARM64 is not compatible with GO
    });

    bucket.grantRead(convertLambda, 'landing');
    bucket.grantWrite(convertLambda, 'curated');
  }
}
