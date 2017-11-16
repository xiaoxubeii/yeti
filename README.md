# AWS Billing Engine

## Overview
AWS Billing Engine is a storage and analysis engine for AWS Billing Management. Currently supports resource type, usage type, operation, quantity, rate and cost of AWS Billing reports. And also supports customized tags for mulitple analysis dimension.

### Architecture
![](/screenshots/awsbilling-arc.png)

### Components

| Component  | Purpose  |
|:-------------:|:-------------:|
| anser | base framework |
| anser-bill | core module |
| postgresql | nosql database |
| boto3 | AWS provider |
| rq-scheduler | task scheduling |
| redis | task scheduling and cache |

## Getting Started
### Intallation
### Configuration
### Run
