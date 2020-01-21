"""Update any presigned URLs which will expire too soon"""
import boto3
import ConfigParser
import json
import logging
import os
import re
import time

try:
    from urlparse import urlsplit
except ImportError: # Python 3
    from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

CONFIG = ConfigParser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

THRESHOLD = 5*24*60*60

def _get_expiry(url):
    """
    Return the expiry time of a presigned URL
    """
    expires = 0
    match = re.search('Expires=(\d\d\d\d\d\d\d\d\d\d)', url)
    if match:
        expires = int(match.group(1))
    return expires

def _create_presigned_url(method, object_name, duration_in_seconds=600):
    """
    Create presigned S3 URL
    """
    s3_client = boto3.client('s3',
                             endpoint_url=CONFIG.get('s3', 'url'),
                             aws_access_key_id=CONFIG.get('s3', 'access_key_id'),
                             aws_secret_access_key=CONFIG.get('s3', 'secret_access_key'))
    if method == 'get':
        try:
            response = s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket':CONFIG.get('s3', 'bucket'), 'Key': object_name},
                                                        ExpiresIn=duration_in_seconds)
        except Exception:
            logger.critical('Unable to generate presigned url for get')
            return None
    else:
        try:
            response = s3_client.generate_presigned_url('put_object',
                                                        Params={'Bucket':CONFIG.get('s3', 'bucket'), 'Key':object_name},
                                                        ExpiresIn=duration_in_seconds,
                                                        HttpMethod='PUT')
        except Exception:
            logger.critical('Unable to generate presigned url for put')
            return None

    return response

def update_presigned_urls(args, json_file):
    """
    Update & replace any presigned URLs as necessary
    """
    # Check if object storage is enabled; return immediately if it is not
    if CONFIG.get('s3', 'url') == '':
        return args
    
    # Regular expression
    url = CONFIG.get('s3', 'url').replace('/', '\/').replace(':', '\:').replace('.', '\.')
    url_regex = url + '\/' + CONFIG.get('s3', 'bucket') + '\/[\w\/\%\&\=\?\.\-]+'

    # Replace any presigned URLs in args
    new_args = args
    matches = re.findall('(%s)' % url_regex, args)
    if matches:
        for match in matches:
            if _get_expiry(match) - time.time() < THRESHOLD:
                logger.info('Replacing a presigned URL in args')
                object_name = urlsplit(match).path.replace('/%s/' % CONFIG.get('s3', 'bucket'), '')
                new_url = _create_presigned_url('put', object_name, 604800)
                new_args = new_args.replace(match, new_url)

    # Replave any presigned URLs in the mapped json file
    try:
        with open(json_file, 'r') as json_fd:
            job_json = json.load(json_fd)
    except Exception as err:
        logger.critical('Unable to open the mapped json file due to %s', err)
        return None

    changes = 0

    artifacts_new = []
    if 'artifacts' in job_json:
        artifacts = job_json['artifacts']
        for artifact in artifacts:
            if 'url' in artifact:
                matches = re.findall('(%s)' % url_regex, artifact['url'])
                if matches:
                    for match in matches:
                        if _get_expiry(match) - time.time() < THRESHOLD:
                            logger.info('Replacing a presigned URL in artifacts')
                            changes += 1
                            object_name = urlsplit(match).path.replace('/%s/' % CONFIG.get('s3', 'bucket'), '')
                            artifact['url'] = _create_presigned_url('get', object_name, 604800)
            artifacts_new.append(artifact)
        job_json['artifacts'] = artifacts_new

    output_files_new = []
    if 'outputFiles' in job_json:
        output_files = job_json['outputFiles']
        for output_file in output_files:
            if 'url' in output_file:
                matches = re.findall('(%s)' % url_regex, output_file['url'])
                if matches:
                    for match in matches:
                        if _get_expiry(match) - time.time() < THRESHOLD:
                            logger.info('Replacing a presigned URL in output files')
                            changes += 1
                            object_name = urlsplit(match).path.replace('/%s/' % CONFIG.get('s3', 'bucket'), '')
                            output_file['url'] = _create_presigned_url('put', object_name, 604800)
            output_files_new.append(output_file)
        job_json['outputFiles'] = output_files_new

    output_dirs_new = []
    if 'outputDirs' in job_json:
        output_dirs = job_json['outputDirs']
        for output_dir in output_dirs:
            if 'url' in output_dir:
                matches = re.findall('(%s)' % url_regex, output_dir['url'])
                if matches:
                    for match in matches:
                        if _get_expiry(match) - time.time() < THRESHOLD:
                            logger.info('Replacing a presigned URL in output directories')
                            changes += 1
                            object_name = urlsplit(match).path.replace('/%s/' % CONFIG.get('s3', 'bucket'), '')
                            output_dir['url'] = _create_presigned_url('put', object_name, 604800)
            output_dirs_new.append(output_dir)
        job_json['outputDirs'] = output_dirs_new

    # Update the mapped json file if necessary
    if changes > 0:
        json_file_tmp = '%s-tmp' % json_file
        try:
            with open(json_file_tmp, 'w') as json_fd:
                json.dump(job_json, json_fd)
        except IOError as err:
            logger.critical('Unable to write new mapped json file to a temporary file due to %s', err)
            return None

        try:
            os.rename(json_file_tmp, json_file)
        except IOError as err:
            logger.critical('Unable to rename mapped json file due to %s', err)
            return None

    return new_args
