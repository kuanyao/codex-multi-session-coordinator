import assert from "node:assert/strict";
import test from "node:test";
import { App } from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { CoordinationStack } from "../lib/coordination-stack.js";

test("coordination stack creates a retained encrypted on-demand table", () => {
  const stack = new CoordinationStack(new App(), "TestCoordination", {
    stage: "dev",
    scope: "aurora",
    env: { account: "111122223333", region: "us-east-1" }
  });
  const template = Template.fromStack(stack);
  template.resourceCountIs("AWS::DynamoDB::Table", 1);
  template.hasResourceProperties("AWS::DynamoDB::Table", {
    TableName: "codex-multi-session-coordinator-dev",
    BillingMode: "PAY_PER_REQUEST",
    PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true },
    TimeToLiveSpecification: { AttributeName: "ttl", Enabled: true }
  });
  const table = Object.values(template.findResources("AWS::DynamoDB::Table"))[0] as any;
  assert.equal(table.DeletionPolicy, "Retain");
  assert.equal(table.UpdateReplacePolicy, "Retain");
});
