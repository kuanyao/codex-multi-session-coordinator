import { CfnOutput, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import { Construct } from "constructs";

export interface CoordinationStackProps extends StackProps {
  readonly stage: string;
  readonly scope: string;
}

export class CoordinationStack extends Stack {
  readonly table: dynamodb.Table;

  constructor(scope: Construct, id: string, props: CoordinationStackProps) {
    super(scope, id, props);
    this.table = new dynamodb.Table(this, "CoordinationTable", {
      tableName: `codex-multi-session-coordinator-${props.stage}`,
      partitionKey: { name: "scope", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "record_id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      timeToLiveAttribute: "ttl",
      removalPolicy: RemovalPolicy.RETAIN
    });

    new CfnOutput(this, "CoordinationTableName", { value: this.table.tableName });
    new CfnOutput(this, "CoordinationTableArn", { value: this.table.tableArn });
    new CfnOutput(this, "CoordinationScope", { value: props.scope });
  }
}
