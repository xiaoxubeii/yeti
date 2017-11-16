# yeti - A AWS Billing analysis system.

## Overview
yeti is a storage and analysis engine for AWS Billing Management. Currently supports resource type, usage type, operation, quantity, rate and cost of AWS Billing reports. And also supports customized tags for mulitple analysis dimension.

### Architecture
![](/screenshots/yeti-arc.png)

### Components

| Component  | Purpose  |
|:-------------:|:-------------:|
| anser | base framework |
| yeti | core module |
| postgresql | nosql database |
| boto3 | AWS provider |
| rq-scheduler | task scheduling |
| redis | task scheduling and cache |

## Getting Started
### Intallation
### Configuration
### Run
