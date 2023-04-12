# Technical assesment 

# Problem solving
## Assumptions
Common-preference
1. The minimum number any value can be is the length of the value
    e.g the prefix will always start with a length of 0 with the suffix having a len of the value
2. You want the maximum amount of matachable text. 
    eg if the text is abc and the match is abc. That is 3, rather than 1 as a matches with a.

All tests are contained in the folders of the code it is testing. I usually do this for Lambda functions to put the tests in the same place as the function (especially if there is multiple functions in the same code base). This way it uploads to AWS, so if anyone needs to check my lambda function they have more code/info to debug. Costs a little in space, but minimal compared to the value. 

# AWS infrastructure
## CSV ingestion pipeline 

### Basic stack
csv -> s3 -> dynamodb -> api gateway

### Visulisation stack
s3 -> Quicksight -> dashboard
[Quicksight dashboard](https://eu-central-1.quicksight.aws.amazon.com/sn/dashboards/6b406573-0b54-4377-b13f-9ce15aa63178/sheets/6b406573-0b54-4377-b13f-9ce15aa63178_a7b1db2a-b95d-407b-b5ef-fa9dd74b7b89)

### Infrastrucutre diagram
![alt text](assets/infrastructure_diagram.png)


