import { Construct } from 'constructs';
import { Duration } from 'aws-cdk-lib';
import { IRole } from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';

export const OS = 'linux'; 
export const ARCH = 'amd64'; 

export interface GoLambdaProps {
	sourceFolder: string;
	memorySize: number;
	timeout: Duration;

	environment?: { [key: string]: string }
	role?: IRole;
	layers?: lambda.ILayerVersion[];
}

export class GoLambda extends lambda.Function {
	constructor(scope: Construct, id: string, props: GoLambdaProps) {
		// To package a Go lambda with CDK, certain props have to be set exactly right
		
		// Ensure the minimum env variables are set
		if (props.environment == null) {
			props.environment = {}
		}
		props.environment.GOOS = OS;
		props.environment.GOARCH = ARCH;
		props.environment.CGO_ENABLED = '0';
				
		// Create the Go lambda
		const allProps = {
			code: lambda.Code.fromAsset(props.sourceFolder, {
				bundling: {
					image: lambda.Runtime.GO_1_X.bundlingImage,
					environment: props.environment,
					user: "root",
					command: [
						'bash', '-c', [
							'go test -v',
							`GOOS=${OS} GOARCH=${ARCH} go build -o /asset-output/main`,
						].join(' && '),
					]
				}
			}),
			architecture: lambda.Architecture.X86_64,
			runtime: lambda.Runtime.GO_1_X,
			handler: 'main',

			memorySize: props.memorySize,
			layers: props.layers,
			timeout: props.timeout,
			role: props.role,
		};
		
		super(scope, id, allProps);
	}
}