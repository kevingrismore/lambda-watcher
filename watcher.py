"""
----------------- Prerequisites -----------------
1. Async lambda invocations
This service is designed to be used with AWS Lambda functions that are invoked asynchronously.
The async invocation requirement is present because Lambda completion messages can only be forwarded to
even processing systems like an EventBridge event bus when the invocation is asynchronous.

2. AWS CloudTrail
A CloudTrail trail must be set up to capture the invocations of the Lambda functions.

3. EventBridge rules
EventBridge rules must be set up to forward Lambda completion events and CloudTrail events for Lambda
invocations to an SQS queue for consumption by this service.

4. FIFO SQS queue
This service polls an SQS queue for messages and parses their contents to create flow runs and manage their state.

----------------- Limitations -----------------
This approach cannot capture the following:
- Logs and prints from the Lambda function
- Retries after a Lambda function fails
- Lambda functions that are invoked synchronously
"""



import abc
import asyncio
import json
from datetime import datetime
from enum import Enum
from uuid import UUID

import boto3
from prefect import flow, get_client
from prefect.states import Running, Completed, Failed
from pydantic import BaseModel


class Invocation(BaseModel):
    lambda_name: str
    flow_run_id: UUID | None
    start_time: datetime | None
    end_time: datetime | None
    success: bool | None

    async def create_or_update_flow_run(self):
        # we saw the invocation end after the start
        if self.flow_run_id and self.end_time:
            await self.set_terminal_state()

        # we saw the invocation start before the end
        elif self.start_time and not self.end_time:
            flow_run = await self.create_run()
            self.flow_run_id = flow_run.id

        # we saw the invocation end before the start
        elif self.start_time and self.end_time:
            flow_run = await self.create_run()
            self.flow_run_id = flow_run.id
            await self.set_terminal_state()

    async def create_run(self):
        return await get_client().create_flow_run(
            flow=lambda_flow.with_options(name=self.lambda_name),
            state=Running(timestamp=self.start_time),
        )

    async def set_terminal_state(self):
        await get_client().set_flow_run_state(
            flow_run_id=self.flow_run_id,
            state=Completed(timestamp=self.end_time)
            if self.success
            else Failed(timestamp=self.end_time),
        )


class MessageType(Enum):
    START = "AWS API Call via CloudTrail"
    SUCCESS = "Lambda Function Invocation Result - Success"
    FAILURE = "Lambda Function Invocation Result - Failure"


class LambdaMessage(BaseModel, abc.ABC):
    message: dict

    def get_timestamp(self) -> datetime:
        return datetime.strptime(self.message["time"], "%Y-%m-%dT%H:%M:%SZ")

    @abc.abstractmethod
    def get_request_id(self) -> str: ...

    @abc.abstractmethod
    def get_lambda_name(self) -> str: ...


class StartMessage(LambdaMessage):
    def get_request_id(self) -> str:
        print(self.message)
        return self.message["detail"]["requestID"]

    def get_lambda_name(self) -> str:
        return self.message["detail"]["requestParameters"]["functionName"].split(":")[
            -1
        ]


class DestinationMessage(LambdaMessage):
    def get_request_id(self) -> str:
        print(self.message)
        return self.message["detail"]["requestContext"]["requestId"]

    def get_lambda_name(self) -> str:
        return self.message["detail"]["requestContext"]["functionArn"].split(":")[-2]


class QueueWatcher:
    def __init__(self, queue_url: str):
        self.sqs = boto3.client("sqs")
        self.queue_url = queue_url
        self.invocations: dict[str, Invocation] = dict()

    def _update_invocation(
        self,
        request_id: str,
        message_type: MessageType,
        timestamp: datetime,
    ) -> Invocation:
        if message_type == MessageType.START:
            self.invocations[request_id].start_time = timestamp

        elif message_type == MessageType.SUCCESS or message_type == MessageType.FAILURE:
            self.invocations[request_id].end_time = timestamp
            self.invocations[request_id].success = message_type == MessageType.SUCCESS

        return self.invocations[request_id]

    def _add_invocation(
        self,
        request_id: str,
        message_type: MessageType,
        timestamp: datetime,
        lambda_name: str,
    ) -> Invocation:
        if message_type == MessageType.START:
            self.invocations[request_id] = Invocation(
                lambda_name=lambda_name, start_time=timestamp
            )

        elif message_type == MessageType.SUCCESS or message_type == MessageType.FAILURE:
            self.invocations[request_id] = Invocation(
                lambda_name=lambda_name,
                end_time=timestamp,
                success=message_type == MessageType.SUCCESS,
            )

        return self.invocations[request_id]

    async def _handle_message(self, raw_message: dict):
        message_type = MessageType(raw_message["detail-type"])

        if message_type == MessageType.START:
            message = StartMessage(message=raw_message)

        elif message_type == MessageType.SUCCESS or message_type == MessageType.FAILURE:
            message = DestinationMessage(message=raw_message)

        request_id = message.get_request_id()
        timestamp = message.get_timestamp()
        lambda_name = message.get_lambda_name()

        if request_id not in self.invocations:
            invocation = self._add_invocation(
                request_id, message_type, timestamp, lambda_name
            )

        else:
            invocation = self._update_invocation(request_id, message_type, timestamp)

        completed = await invocation.create_or_update_flow_run()

        if completed:
            finished_invocation = self.invocations.pop(request_id)
            print(
                f"Invocation {finished_invocation.lambda_name} with flow run {finished_invocation.flow_run_id} completed"
            )

    async def _receive_messages(self):
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            AttributeNames=["SentTimestamp"],
            MaxNumberOfMessages=10,
            MessageAttributeNames=["All"],
            WaitTimeSeconds=20,
        )

        messages = response.get("Messages", [])

        for message in messages:
            body = json.loads(message["Body"])

            self.sqs.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"]
            )

            await self._handle_message(body)

    async def run(self):
        while True:
            print("Checking for new messages...")
            await self._receive_messages()


@flow
def lambda_flow():
    pass


if __name__ == "__main__":
    watcher = QueueWatcher(
        queue_url="<your-sqs-queue-url>"
    )
    asyncio.run(watcher.run())
