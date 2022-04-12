import { Duration, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamo from 'aws-cdk-lib/aws-dynamodb';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import { GoLambda } from '../go_constructs/go-lambda';
import * as path from 'path';
import { AttributeType } from 'aws-cdk-lib/aws-dynamodb';

export class GoGoDataLakeStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const bucket = new s3.Bucket(this, 'go-go-bucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      publicReadAccess: false
    });

    const database = new dynamo.Table(this, 'go-data-lake', {
      partitionKey: {
        name: "Day",
        type: dynamo.AttributeType.NUMBER
      },
      encryption: dynamo.TableEncryption.AWS_MANAGED,
      readCapacity: 1,
      writeCapacity: 1,
    });

    const convertLambda = new GoLambda(this, 'convert-lambda', {
      sourceFolder: path.join(__dirname, '../src/convert'),
      memorySize: 256,
      timeout: Duration.minutes(1),
    });

    bucket.grantRead(convertLambda, 'landing/*');
    bucket.grantWrite(convertLambda, 'curated/*');

    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(convertLambda),
      s3.NotificationKeyFilter(
        prefix="landing/",
        suffix="*.csv",
    ),
    );

    const dynamoLambda = new GoLambda(this, 'dynamo-lambda', {
      sourceFolder: path.join(__dirname, '../src/dynamo'),
      memorySize: 256,
      timeout: Duration.minutes(1),
      environment: {
        TABLE_NAME: database.tableName
      }
    });

    bucket.grantRead(dynamoLambda, 'curated/*');
    database.grantWriteData(dynamoLambda);

    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(dynamoLambda),
      s3.NotificationKeyFilter(
        prefix="curated/",
        suffix="*.parquet",
    ),
    );
  }
}
