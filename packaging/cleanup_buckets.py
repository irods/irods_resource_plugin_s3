#!/usr/bin/env python
from __future__ import print_function
import argparse
import os
import boto3
from botocore.client import Config


ci_keys_file='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
s3region = 'us-east-1'

# arg parsing
parser = argparse.ArgumentParser()
parser.add_argument('-c', '--configure', action="store_true", help='set up aws configuration')

args = parser.parse_args()

def set_up_aws_config_dir(keys_file):
    # read access keys from keypair file
    with open(keys_file) as f:
        aws_access_key_id = f.readline().rstrip()
        aws_secret_access_key = f.readline().rstrip()

    # make config dir
    aws_cfg_dir_path = os.path.join(os.path.expanduser('~'), '.aws')
    try:
        os.makedirs(aws_cfg_dir_path)
    except OSError:
        if not os.path.isdir(aws_cfg_dir_path):
            raise

    # make config file
    with open(os.path.join(aws_cfg_dir_path, 'config'), 'w') as cfg_file:
        cfg_file.write('[default]\n')
        cfg_file.write('region=' + s3region + '\n')

    # make credentials file
    with open(os.path.join(aws_cfg_dir_path, 'credentials'), 'w') as cred_file:
        cred_file.write('[default]\n')
        cred_file.write('aws_access_key_id = ' + aws_access_key_id + '\n')
        cred_file.write('aws_secret_access_key = ' + aws_secret_access_key + '\n')


# set up config files if needed
if args.configure:
    set_up_aws_config_dir(ci_keys_file)

s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))

# cleanup buckets
for bucket in s3.buckets.all():
        if bucket.name.startswith('irods-ci-ubuntu') or bucket.name.startswith('irods-ci-centos') or bucket.name.startswith('irods-ci-opensuse'):
                for obj in bucket.objects.all():
                        obj.delete()
                print('deleting {bucket.name}'.format(**locals()))
                bucket.delete()
