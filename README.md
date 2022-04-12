# go-go-data-lake
This repo is a proof-of-concept serverless data lake built on AWS. All the ETL jobs (Lambdas) are 100% written in Go, and the CI/CD pipeline is implemented in CDK TypeScript.
Go was not used for the CDK component as, at the time of writing, it has not implemented enough features for this POC to be viable.

## Behaviour

This POC ingests a CSV file detailing Russian equipment losses in the current Russia-Ukraine conflict. When the file is uploaded to the `Landing` zone it is converted to Parquet and written to `Curated`, then a second Lambda picks it up and writes all rows to a DynamoDB table. All of this is just to test the ability and maturity of Go for cloud-native data wrangling, we are not doing any fancy or meaningful data science here.

## Shoulders of Giants

Huge shoutout to the community-driven open source packages that make projects like this viable:
- [parquet-go](https://github.com/xitongsys/parquet-go)
- [aws-sdk-go](https://github.com/aws/aws-sdk-go)
- [gota](https://github.com/go-gota/gota)

## Development

To develop on this, simply start hacking. All Lambda source code is in the `src/` directory, with each sub directory specifying a different Lambda function.
The stacks defined in `lib/` directory are the core of the CDK application that actually creates and deploys resources to AWS.

## Deployment

First auth to your AWS environment, then make sure you have Docker running locally. Finally, just run:
```
cdk deploy
```
Amazing!

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk synth`       emits the synthesized CloudFormation template
