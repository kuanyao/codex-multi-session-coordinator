#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { CoordinationStack } from "../lib/coordination-stack.js";

const app = new cdk.App();
const stage = app.node.tryGetContext("stage") || process.env.CODEX_COORDINATOR_STAGE || "dev";
const scope = app.node.tryGetContext("scope") || process.env.CODEX_COORDINATOR_SCOPE || "default";
new CoordinationStack(app, `CodexMultiSessionCoordinator-${stage}`, {
  stage,
  scope,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT || "934347069392",
    region: process.env.CDK_DEFAULT_REGION || "us-east-1"
  },
  description: `DynamoDB coordination lease service for Codex sessions (${stage}).`
});
