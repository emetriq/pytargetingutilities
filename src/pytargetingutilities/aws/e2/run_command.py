import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List

import boto3
import botocore


class RunCommandEc2:

    max_pool_connections = 60

    @staticmethod
    def run_commands_and_wait(
        cmds: List[str],
        ec2_ids: List[str],
        delay,
        max_attempts,
        wait_for_invocation,
        timeout_seconds=60 * 60,
    ):
        response = RunCommandEc2.run_commands(ec2_ids, cmds, timeout_seconds)
        command_id = response['Command']['CommandId']
        waiter_args = [
            {
                'CommandId': command_id,
                'InstanceId': ec2_id,
                'PluginName': 'aws:RunShellScript',
                'WaiterConfig': {'Delay': delay, 'MaxAttempts': max_attempts},
            }
            for ec2_id in ec2_ids
        ]
        return RunCommandEc2._wait_for_result(
            command_id, ec2_ids, wait_for_invocation, waiter_args
        )

    @staticmethod
    def _wait_for_result(
        command_id, ec2_ids, wait_for_invocation, waiter_args
    ):
        waiter = boto3.client(
            'ssm',
            config=RunCommandEc2._create_config(
                RunCommandEc2.max_pool_connections
            ),
        ).get_waiter('command_executed')
        # we can only wait for one instance and to wait for all we need a pool
        with ThreadPoolExecutor(max_workers=len(waiter_args)) as executor:
            for arg in waiter_args:
                executor.submit(waiter.wait, **arg)
        outputs = RunCommandEc2._get_command_invocation(
            command_id, ec2_ids, wait_for_invocation
        )
        # wait until all tasks are completed
        if any(map(lambda x: x['Status'] == 'InProgress', outputs)):
            return RunCommandEc2._wait_for_result(
                command_id, ec2_ids, wait_for_invocation, waiter_args
            )
        return outputs

    @staticmethod
    def _get_command_invocation(command_id, ec2_ids, wait_for_invocation):
        ssm_client = boto3.client(
            'ssm',
            config=RunCommandEc2._create_config(
                RunCommandEc2.max_pool_connections
            ),
        )
        # sleep prevents for InvocationDoesNotExist error
        time.sleep(wait_for_invocation)
        outputs = [
            ssm_client.get_command_invocation(
                CommandId=command_id, InstanceId=ec2_id
            )
            for ec2_id in ec2_ids
        ]
        return outputs

    @staticmethod
    def run_commands(
        instance_ids: List[str], commands: List[str], timeout_seconds=60 * 60
    ):
        ssm_client = boto3.client(
            'ssm',
            config=RunCommandEc2._create_config(
                RunCommandEc2.max_pool_connections
            ),
        )
        response = ssm_client.send_command(
            InstanceIds=instance_ids,
            TimeoutSeconds=timeout_seconds,
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': [' && '.join(commands)]},
        )
        return response

    @staticmethod
    def _create_config(max_pool_connections):
        return botocore.config.Config(
            max_pool_connections=max_pool_connections,
        )
