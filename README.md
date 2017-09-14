# aws_project

The system run in two Amazon Availability Zones (AZs), making it robust to failure of any one AZ.

The system will handle duplicate messages.

The replicated data will increase durability.

Architecture
The architecture comprises a frontend, five SQS queues, two replicated backends, two replicated duplicators, and two DynamoDB tables:
