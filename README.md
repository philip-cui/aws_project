# aws_project

The system run in two Amazon Availability Zones (AZs), making it robust to failure of any one AZ.

The system handles duplicate messages.

The replicated data increases durability.

Architecture
The architecture comprises a frontend, five SQS queues, two replicated backends, two replicated duplicators, and two DynamoDB tables:
![alt text](https://github.com/philip-cui/aws_project/blob/master/replicated-arch.png)
