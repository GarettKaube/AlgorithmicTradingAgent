import boto3
import json
import time

def lambda_handler(event, context):
    action = event['action']

    s3_bucket = "mybucket1654"

    s3_client = boto3.client('s3')
    ec2 = boto3.client('ec2')

    response = s3_client.get_object(Bucket=s3_bucket, Key="vpc.json")
    content = response['Body'].read().decode('utf-8')
    vpc_conf = json.loads(content)

    vpc_ip = vpc_conf["vpc_id"]
    public_subnets = vpc_conf['subnets']['public']
    route_tables = vpc_conf['route_tables']

    if action == "create":
        # Allocate an Elastic IP for the NAT Gateway
        eip = ec2.allocate_address(Domain='vpc')
        eip_allocation_id1 = eip['AllocationId']

        eip = ec2.allocate_address(Domain='vpc')
        eip_allocation_id2 = eip['AllocationId']

        eips = [eip_allocation_id1, eip_allocation_id2]

        # Create NAT gateways for each subnet
        nat_gateways = []
        for eip, pub_subnet in zip(eips, public_subnets):
            response = ec2.create_nat_gateway(
                AllocationId=eip,
                SubnetId=pub_subnet
            )
            nat_gateway_id = response['NatGateway']['NatGatewayId']
            nat_gateways.append(nat_gateway_id)

        # Allocate the NAT gateways to the route tables associated with the
        # private subnets
        status = None
        for natgw, table in zip(nat_gateways, route_tables):
            response = ec2.describe_nat_gateways(NatGatewayIds=[natgw])
            status = response['NatGateways'][0]['State']

            if status != 'available':
                print('Waiting for NAT gateway to become available')

            while status != 'available':
                response = ec2.describe_nat_gateways(NatGatewayIds=[natgw])
                status = response['NatGateways'][0]['State']
                time.sleep(5)

            print(f'Creating route for {table}')
            ec2.create_route(
                RouteTableId=table,
                DestinationCidrBlock='0.0.0.0/0', 
                NatGatewayId=natgw
            )
        print('Done')
        return "Creation sucessful"

    if action == "delete":
        response = ec2.describe_nat_gateways(
            Filter=[
                {
                    'Name': 'vpc-id',
                    'Values': [vpc_ip] 
                },
                {
                    'Name': 'state',
                    'Values': ['available', 'deleting']
                }
            ]
        )

        nat_gateway_ids = [nat['NatGatewayId'] for nat in response['NatGateways']]
        adresses = ec2.describe_addresses()['Addresses']
        allocation_ids = [item['AllocationId'] for item in adresses]
        
        
        for natgw, route_table_id in zip(nat_gateway_ids, route_tables):
            
            ec2.delete_nat_gateway(NatGatewayId=natgw)
            ec2.delete_route(
                RouteTableId=route_table_id,
                DestinationCidrBlock='0.0.0.0/0'
        )

        # Wait for NAT gateways to finish deleting or else we cannot release
        # the elastic IPs.
        print('Waiting for NAT gateways to delete')
        deleted = [False, False]
        while not all(deleted):
            if nat_gateway_ids == []:
                deleted = [True, True]
            for i, nat in enumerate(nat_gateway_ids):
                response = ec2.describe_nat_gateways(NatGatewayIds=[nat])
                status = response['NatGateways'][0]['State']
                if status == 'deleted':
                    deleted[i] = True


        print('Releasing Elastic IPs')
        for id in allocation_ids:
            ec2.release_address(AllocationId=id)
        return "Delete sucessful"