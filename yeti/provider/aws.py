#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.common.log import LOG
from yeti.provider import BaseProvider
import boto3
import copy
from yeti.db.model import Size
from yeti.common.utils import id_generator
from uuid import uuid4
import io
from yeti import CONF
import os
import pandas as pd
from subprocess import Popen
from datetime import datetime as dt
from yeti.provider.yeti import yetiPoller

import base64


class AWS(BaseProvider, yetiPoller):
    def __init__(self, **kwargs):
        super(AWS, self).__init__()
        self.ec2client = boto3.client('ec2', aws_access_key_id=kwargs['user'], aws_secret_access_key=kwargs['key'],
                                      region_name=kwargs['region'])
        self.ec2resource = boto3.resource('ec2', aws_access_key_id=kwargs['user'], aws_secret_access_key=kwargs['key'],
                                          region_name=kwargs['region'])
        self.elbclient = boto3.client('elb', aws_access_key_id=kwargs['user'], aws_secret_access_key=kwargs['key'],
                                      region_name=kwargs['region'])
        self.s3client = boto3.client('s3', aws_access_key_id=kwargs['user'], aws_secret_access_key=kwargs['key'],
                                     region_name=kwargs['region'])

    def create_instance(self, res):
        """
        :param res:
        {
          "name": "",
          "image_id": "",
          "network_id": "",
          "subnet_id": "",
          "count": 1,
          "size": {
            "disk": 1,
            "name": ""
          },
          "secg_rules": [
            {
              "protocol": "tcp",
              "from_port": 1,
              "to_port": 1,
              "source": "0.0.0.0/0"
            }
          ]
        }
        :return:
        [{
          "id": "",
          "private_ip": "",
          "name": "",
          "image_id": "",
          "network_id": "",
          "subnet_id": "",
          "count": 1,
          "state": "",
          "size": {
            "disk": 1,
            "name": ""
          },
          "secg_rules": [
            {
              "protocol": "tcp",
              "from_port": 1,
              "to_port": 1,
              "source": "0.0.0.0/0"
            }
          ]
        }]
        """
        default_dev_name = '/dev/sda1'
        default_del_term = True

        root_size = res['root_disk']
        size_name = res['size_id']

        new_instances = []
        count = int(res.get('count', 1))
        hostname_prefix = res['hostname']
        hostname_prefix = '%s.%s' % (hostname_prefix, 'tendcloud.com')
        index = res['max_num']

        for i in xrange(count):
            index += 1
            try:
                hostname = hostname_prefix % index
            except TypeError:
                pass

            result = self.ec2client.run_instances(ImageId=res['image_id'], MinCount=1, MaxCount=1,
                                                  InstanceType=size_name,
                                                  SubnetId=res['subnet_id'],
                                                  UserData="hostname:%s" % hostname,
                                                  SecurityGroupIds=[],
                                                  BlockDeviceMappings=[{'DeviceName': default_dev_name,
                                                                        'Ebs': {'VolumeSize': int(root_size),
                                                                                'DeleteOnTermination': default_del_term}}])

            i = result['Instances'][0]
            tmp = copy.deepcopy(res)

            tmp.pop('count')
            tmp.update({'hostname': hostname, 'state': i['State']['Name'], 'id': i['InstanceId'],
                        'fixed_ip': i['PrivateIpAddress'], 'seq': index})
            self.ec2client.create_tags(Resources=[i['InstanceId']], Tags=[{'Key': 'Name', 'Value': hostname}])
            new_instances.append(tmp)

        return new_instances

    def create_tags(self, instance_id, tags):
        '''
            Create tag from aws
            instance_id:
            tags:
        '''
        new_aws_tags = []

        for k, v in tags.items():
            new_aws_tags_dict = {}

            new_aws_tags_dict['Key'] = k
            new_aws_tags_dict['Value'] = v

            new_aws_tags.append(new_aws_tags_dict)

        self.ec2client.create_tags(Resources=[instance_id], Tags=new_aws_tags)

    def create_secgroup(self, secgroup):
        data = secgroup.pop("data")

        secg_result = self.ec2client.create_security_group(GroupName=data['name'], Description=data['description'],
                                                           VpcId=data['vpc_id'])

        secgroup['id'] = secg_result['GroupId']
        secgroup['instance_id'] = secg_result['GroupId']
        rules = secgroup.get('rules')
        if rules:
            main_rule = {'GroupId': secgroup['id'], 'IpPermissions': []}
            for rule in rules:
                kwargs = {}
                kwargs['IpProtocol'] = rule['protocol'] if rule.get('protocol') else '-1'

                if kwargs['IpProtocol'] != -1:
                    if kwargs['IpProtocol'] in ('tcp', 'udp'):
                        from_port = rule.get('from_port')
                        to_port = rule.get('to_port')
                        if from_port and to_port and from_port != -1 and to_port != -1:
                            kwargs['FromPort'] = int(rule['from_port'])
                            kwargs['ToPort'] = int(rule['to_port'])
                        else:
                            kwargs['FromPort'] = 1
                            kwargs['ToPort'] = 65535
                    elif kwargs['IpProtocol'] == 'icmp':
                        kwargs['FromPort'] = kwargs['ToPort'] = -1

                kwargs['IpRanges'] = [{'CidrIp': rule.get('cidr') if rule.get('cidr') else '0.0.0.0/0'}]
                main_rule['IpPermissions'].append(kwargs)

            self.ec2client.authorize_security_group_ingress(**main_rule)

        return secgroup

    def bind_secgs(self, new_sec_group_ids, server_ids):
        for server_id in server_ids:
            self.ec2client.modify_instance_attribute(InstanceId=server_id, Groups=new_sec_group_ids)

    def bind_eips(self, insids):
        ins_eip_array = []
        for id in insids:
            ins_eip_map = {}
            eip = self.ec2client.allocate_address(Domain='vpc')
            self.ec2client.associate_address(InstanceId=id, AllocationId=eip['AllocationId'])

            ins_eip_map['ins_id'] = id
            ins_eip_map['eip_id'] = eip['AllocationId']
            ins_eip_map['eip'] = eip['PublicIp']
            ins_eip_array.append(ins_eip_map)

        return ins_eip_array

    def get_instance(self, id):
        def to(m, id):
            if m:
                # retrieve server detail except volumes and security group rules
                mac = None
                ifs = m['NetworkInterfaces']
                if ifs and len(ifs):
                    # only support one interface
                    mac = ifs[0]['MacAddress']

                data = {'id': m['InstanceId'], 'image_id': m['ImageId'],
                        'state': self.format_state(state=m['State']['Name']),
                        'size': {'name': m['InstanceType']},
                        'subnet_id': m['SubnetId'], 'fixed_ip': m['PrivateIpAddress'],
                        'network_id': m['VpcId'], 'mac': mac}
                hostname = self.get_instnace_attr(id)
                if hostname:
                    data.update({
                        "hostname": hostname
                    })
                return data

        result = self.ec2client.describe_instances(InstanceIds=[id])
        return to(result['Reservations'][0]['Instances'][0], id)

    def get_instnace_attr(self, id):
        attr = self.ec2client.describe_instance_attribute(Attribute="userData", InstanceId=id)
        if attr.get("UserData"):
            encode_name = attr.get("UserData")['Value']
            decode_name = base64.b64decode(encode_name).split(":")
            if len(decode_name) > 1:
                return decode_name[1]

    def sync_tags(self, hostname, id, tags):
        decode_name = self.get_instnace_attr(id)
        if decode_name and hostname != decode_name:
            tags['Name'] = decode_name
            self.create_tags(id, tags)
            return True, tags
        return False, False

    def format_state(self, **kwargs):
        in_state = kwargs['state']
        out_state = None
        if in_state == 'pending':
            out_state = 'building'
        elif in_state in ('shutting-down', 'stopping'):
            out_state = 'stopping'
        elif in_state == 'stopped':
            out_state = 'stopped'
        elif in_state == 'running':
            out_state = 'running'

        return out_state

    def list_images(self):
        images = self.ec2client.describe_images(Filters=[{'Name': 'is-public', 'Values': ['false']}])
        return [{'id': i['ImageId'], 'name': i['Name']} for i in images['Images']]

    def list_sizes(self):
        sizes = []
        for s in Size.query.all():
            sizes.append({'id': s.id, 'name': s.name, 'cpu': s.cpu, 'memory': s.memory, 'disk': s.disk,
                          'net_perf': s.net_perf, 'arch': s.arch})

        return sizes

    def start_instance(self, ids):
        self.ec2client.start_instances(InstanceIds=ids)

    def stop_instance(self, ids):
        self.ec2client.stop_instances(InstanceIds=ids)

    def reboot_instance(self, ids):
        self.ec2client.reboot_instances(InstanceIds=ids)

    def reload_instance(self, ids):
        pass

    def register_elb(self, lb_name, server_ids):
        res = self.elbclient.register_instances_with_load_balancer(LoadBalancerName=lb_name,
                                                                   Instances=[{'InstanceId': id} for id in server_ids])
        return res

    def deregister_elb(self, lb_name, server_ids):
        res = self.elbclient.deregister_instances_from_load_balancer(LoadBalancerName=lb_name,
                                                                     Instances=[{'InstanceId': id} for id in
                                                                                server_ids])
        return res

    def list_instances(self):
        """

        :return:
        [
        {
          "image_id": "63bc46bf-d92e-4fac-92f8-14ded42d12d4",
          "mac": "fa:16:3e:a6:c6:00",
          "state": "ACTIVE",
          "size_id": "6",
          "fixed_ip": "172.23.4.7",
          "created_at": "2016-11-08T06:17:38Z",
          "id": "d5c80c38-2618-43fb-b33c-193a12ddcdb7",
          "hostname": "test"
        }
        ]
        """
        result = self.ec2client.describe_instances()
        inses = []
        for r in result['Reservations']:
            inses.extend(r['Instances'])

        return [self._convert_ins(i) for i in inses]

    def _convert_ins(self, ins):
        new_ins = super(AWS, self)._convert_ins(ins)
        net_ifs = ins['NetworkInterfaces']
        if net_ifs:
            net_ifs = net_ifs[0]
            new_ins = {'mac': net_ifs['MacAddress'], 'fixed_ip': net_ifs['PrivateIpAddress']}

        new_ins.update({'id': ins['InstanceId'], 'image_id': ins['ImageId'], 'state': ins['State']['Name'],
                        'size_id': ins['InstanceType'],
                        'created_at': ins['LaunchTime']})

        for tag in ins.get('Tags', []):
            if tag['Key'] == 'Name':
                new_ins['hostname'] = tag['Value']
                break

        eip_data = self.list_elastic_ips(server_id=ins['InstanceId'])
        if eip_data:
            new_ins['eip_data'] = eip_data[0]

        return new_ins

    def list_elastic_ips(self, server_id=None):
        result = None
        elastic_ips = []
        if server_id:
            result = self.ec2client.describe_addresses(Filters=[{"Name": "instance-id",
                                                                 "Values": [server_id]}])
        else:
            result = self.ec2client.describe_addresses()
        if result:
            for eip in result['Addresses']:
                elastic_ips.append({"eip": eip['PublicIp'], 'instance_id': eip['AllocationId']})
        return elastic_ips

    def allocate_elastic_ip(self):
        result = self.ec2client.allocate_address(Domain='vpc')
        return {'elastic_ip': result['PublicIp'], 'instance_id': result['AllocationId']}

    def release_elastic_ip(self, allocation_id):
        result = self.ec2client.release_address(AllocationId=allocation_id)
        return result['ResponseMetadata']['HTTPStatusCode']

    def associate_address(self, server, elip):
        result = self.ec2client.associate_address(AllocationId=elip['instance_id'], InstanceId=server['instance_id'])
        return result['ResponseMetadata']['HTTPStatusCode']

    def disassociate_address(self, server, elip):
        result = self.ec2client.disassociate_address(PublicIp=elip['elastic_ip'])
        return result['ResponseMetadata']['HTTPStatusCode']

    def delete_sec_group(self, instance_id):
        result = self.ec2client.delete_security_group(GroupId=instance_id)
        return result['ResponseMetadata']['HTTPStatusCode']

    def update_secgroup_rules(self, secg, old_rules, new_rules):
        if old_rules:
            ##Revoke ingress rules
            build_old_rules = self._build_data(secg, old_rules)
            aws_secg = self.ec2resource.SecurityGroup(secg['instance_id'])
            aws_secg.revoke_ingress(**build_old_rules)

        if new_rules:
            ##Create ingress rules
            build_new_rules = self._build_data(secg, new_rules)
            self.ec2client.authorize_security_group_ingress(**build_new_rules)

        return new_rules

    def _build_data(self, secg, rules):
        main_rule = {'GroupId': secg['instance_id'], 'IpPermissions': []}
        for rule in rules:
            kwargs = {}
            kwargs['IpProtocol'] = rule['protocol'] if rule.get('protocol') else '-1'

            if kwargs['IpProtocol'] != -1:
                if kwargs['IpProtocol'] in ('tcp', 'udp'):
                    from_port = rule.get('from_port')
                    to_port = rule.get('to_port')
                    if from_port and to_port and from_port != -1 and to_port != -1:
                        kwargs['FromPort'] = int(rule['from_port'])
                        kwargs['ToPort'] = int(rule['to_port'])
                    else:
                        kwargs['FromPort'] = 1
                        kwargs['ToPort'] = 65535
                elif kwargs['IpProtocol'] == 'icmp':
                    kwargs['FromPort'] = kwargs['ToPort'] = -1

            kwargs['IpRanges'] = [{'CidrIp': rule.get('cidr') if rule.get('cidr') else '0.0.0.0/0'}]
            main_rule['IpPermissions'].append(kwargs)
        return main_rule

    def _get_bill(self, datetime):
        bill_csv_path = '%s.csv' % (
            os.path.join(CONF.aws_bill_path, CONF.aws_bill_s3_object) % datetime.strftime('%Y-%m'))

        need_update = False
        if not os.path.exists(bill_csv_path):
            need_update = True
        else:
            now = dt.now()
            update_time = dt.fromtimestamp(os.path.getmtime(bill_csv_path))
            tspan = now - update_time
            if 1 <= tspan.days < 40:
                need_update = True

        if need_update:
            stream = self.s3client.get_object(Bucket=CONF.aws_bill_s3_bucket,
                                              Key='%s.csv' % (
                                                  CONF.aws_bill_s3_object % datetime.strftime('%Y-%m')))
            content = stream['Body'].read().decode('utf-8')
            with io.open(bill_csv_path, 'w', encoding='utf8') as f:
                content = content.splitlines(True)
                f.writelines(content[1:])

        return pd.read_csv(bill_csv_path, encoding='utf8')

    def get_bill(self, datetime):
        return self._get_bill(datetime)

    def get_server_bill(self, datetime):
        df = self._get_bill(datetime)
        df = df[df['ProductCode'] == 'AmazonEC2'].groupby('user:Name')[['TotalCost']].sum()
        df.columns = ['cost']
        df['hostname'] = df.index
        return df

    def get_security_groups_by_name(self, name, vpc_id=None):
        secg = self.ec2client.describe_security_groups(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}, {"Name": "group-name", "Values": [name]}])
        return secg['SecurityGroups']

    def list_tags(self, name, value):
        tags = self.ec2client.describe_tags(Filters=[{'Name': name, 'Values': [value]}])
        return tags.get('Tags')

    def get_server_log(self, server_id):
        try:
            server_log = self.ec2client.get_console_output(InstanceId=server_id)
            if server_log:
                return server_log['Output']
        except Exception,e:
            LOG.info("%s %s" % (server_id, e.message))

    def get_vnc_console(self, server_id):
        return None
