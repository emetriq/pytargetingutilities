import boto3
import logging


class EC2Helper:

    log = logging.getLogger()

    @staticmethod
    def start_instance(instance_config, instance_name):
        """Start instance by given config
        Args:
            instance_config (dict): ec2 config please see section run_instances
            in boto3 doc for more information's
            instance_name (str): name of instance

        Returns:
            [Instance]: instance object
        """
        instance = boto3.client('ec2').run_instances(
            **instance_config,
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': instance_name,
                        }
                    ],
                }
            ],
        )
        return instance

    @staticmethod
    def await_startup(instance_ids):
        """Wait until all instances of the list are in the status okstate

        Args:
            instance_ids (List[str]):: list of instance ids
        """
        waiter = boto3.client('ec2').get_waiter('instance_status_ok')
        waiter.wait(InstanceIds=instance_ids)

    @staticmethod
    def terminate_instances(instance_ids):
        """Terminate ec2 instances by list of instance ids

        Args:
            instance_ids (List[str]): list of instance ids
        """
        boto3.client('ec2').terminate_instances(InstanceIds=instance_ids)

    @staticmethod
    def create_image(instance_id, ami_name, ami_description):
        """Create image of running ec2 instance

        Args:
            instance_id (str): instance id
            ami_name (str): name of ami
            ami_description (str): description of ami

        Returns:
            Image: image object
        """
        return boto3.client('ec2').create_image(
            InstanceId=instance_id, Name=ami_name, Description=ami_description
        )

    @staticmethod
    def is_image_available(image_id):
        """Checks the image status and waits until the image status is no longer pending

        Args:
            name (str): name of ami image

        Returns:
            bool: true if available otherwise false
        """
        client = boto3.client('ec2')
        waiter = client.get_waiter('image_available')
        waiter.wait(Filters=[{'Name': 'image-id', 'Values': [image_id]}])
        image = client.describe_images(ImageIds=[image_id])
        if image['Images'][0]['State'] == 'available':
            EC2Helper.log.info(f'Image {image_id} has invalid state')
            return True
        EC2Helper.log.info(f'Image {image_id} has invalid state')
        return False

    @staticmethod
    def delete_ami_by_name(name):
        """Deletes ami image by name

        Args:
            name (str): name of ami image to delete

        Returns:
            bool: true if delete was successful otherwise false
        """
        client = boto3.client('ec2')
        images = client.describe_images(Owners=["self"])
        for image in images['Images']:
            if name == image['Name']:
                EC2Helper.delete_ami(image["ImageId"])
                return True
        EC2Helper.log.warning(
            f"""Couldn't find image with name {name}. The following images belongs to
             current user: {','.join(map(lambda x: x['Name'], images['Images']))}"""
        )
        return False

    @staticmethod
    def delete_ami(image_id):
        """Deletes ami image by image_id

        Args:
            image_id (str): id of ami image to delete

        Returns:
            bool: true if delete was successful otherwise false
        """
        client = boto3.client('ec2')
        EC2Helper.log.info(
            f'Deregister image {image_id}'
        )
        client.deregister_image(ImageId=image_id)
        return True

    @staticmethod
    def get_latest_image_id(starts_with=''):
        """Returns images id of newest ami
        Args:
            starts_with (str, optional): Filters image ids of account with startswith
            methode. Defaults to '' means filtering is disabled.

        Returns:
            str:  image id of latest ami
        """
        images = EC2Helper.get_image_ids(starts_with)
        if len(images) == 0:
            EC2Helper.log.warning("Didn't find any images in account")
            return None
        return images[-1]

    @staticmethod
    def get_image_ids(starts_with=''):
        """Returns images ids filtered with starts_with and sorted by date
        the last image id is the latest
        Args:
            starts_with (str, optional): Filters image ids of account with startswith
            methode. Defaults to '' means filtering is disabled.

        Returns:
            list[str]: list of image ids
        """
        client = boto3.client('ec2')
        images = client.describe_images(Owners=['self'])['Images']

        if starts_with != '':
            images = list(
                filter(lambda x: x['Name'].startswith(starts_with), images)
            )

        image_ids = list(
            map(
                lambda x: x['ImageId'],
                sorted(images, key=lambda img: img['CreationDate']),
            )
        )
        return image_ids
