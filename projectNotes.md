# Cloud Computing: IaaS project

## Aim
- **Auto scaling of EC2 instances based on the number of requests**
- Able to handle multiple requests concurrently.
- 



## Components
- Frontend to provide user input (which is an image)
- Web server
  -  The most important part as it will register request
  -  Autoscaling should be triggered from here only.
  -  Request will be sent to SQS from here
- SQS (Request Queueing mechanism)
  - We need to maintain a EC2 counter which should not cross 20 instances
  - If ec2Counter == 20, don't dispatch anything from the queue.
- Application Server
  - Business logic is already provided i.e the deep learning model
  - Take image input from SQS.
  - Apply the model on the input
  - Return the output result
  - > Return where?? Via SQS only?
- Data storage
  - > Not able to understand
  - Inputs stored in one S3 bucket
  - Ouputs stored in one S3 bucket in (input, output) pairs
