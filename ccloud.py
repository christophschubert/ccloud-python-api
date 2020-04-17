import json
import os

def call(command):
    f = os.popen('ccloud ' + command)
    o = f.read()
    code = f.close()
    if code:
        print('error' + str(code))

def call_and_parse(command):
    f = os.popen('ccloud ' + command + ' -o json')
    output = f.read()
    res = f.close()
    if res:
        print('error' + str(res))
    return json.loads(output)

def create_jaas(apikey):
    jaas_conf = """sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="{}" password="{}";
ssl.endpoint.identification.algorithm=https
security.protocol=SASL_SSL
sasl.mechanism=PLAIN"""
    return jaas_conf.format(apikey['key'], apikey['secret'])

def list_regions():
    pass

def _add_cluster(cmd, cluster):
    return cmd + ' --cluster ' + cluster if cluster else cmd

def list_clusters():
    pass

def describe_cluster(cluster):
    pass

# topic calls

def list_topics(cluster = None):
    cmd = _add_cluster('kafka topic list', cluster)
    return call_and_parse(cmd)

def delete_topic(topicname, cluster=None):
    pass

def describe_topic(topicname, cluster=None):
    cmd = _add_cluster('kafka topic describe ' + topicname , cluster)
    return call_and_parse(cmd)

def stringify_config(config):
    return ",".join("{}={}".format(k, v) for k, v in config.items())

def create_topic(topicname, cluster=None, partitions=None, config=None):
    p = config.pop('partitions', None)
    if p and not partitions:
        partitions = p

    cmd = _add_cluster('kafka topic create ' + topicname, cluster)
    if partitions:
        cmd += ' --partitions ' + str(partitions)
    if config:
        cmd += ' --config ' + stringify_config(config)
    return call(cmd)

# Service accounts

def create_service_account(name, description):
    """
    returns a dictionary containing id, name, and description of the
    newly created service account
    """
    cmd = 'service-account create {} --description "{}"'.format(name, description)
    return call_and_parse(cmd)

def delete_service_account(id):
    cmd = 'service-account delete {}'.format(id)
    call(cmd)

def list_service_accounts():
    return call_and_parse('service-account list')


def update_service_account(id, description):
    """
    Update description of the service account <id>.
    """
    cmd = 'service-account update {} --description "{}"'.format(id, description)
    call(cmd)

# API keys
def list_api_keys(resource=None, current_user=None, service_account_id=None):
    """
    List API keys. Set any one of the optional parameters to filter for the
    corresponding entries.
    """
    cmd = "api-key list"
    if resource:
        cmd += " --resource {}".format(resource)
    if current_user:
        cmd += " --current-user"
    if service_account_id:
        cmd += " --service-account {}".format(service_account_id)
    return call_and_parse(cmd)

def create_api_key(resource, service_account_id=None, description=None):
    """
    Specify `cloud` as resource when you want an organization-wide useable API key,
    for instance to access the metrics API.

    If no service account is specified, an API with super user access will be created.
    """
    cmd = 'api-key create --resource {}'.format(resource)
    if service_account_id:
        cmd += ' --service-account {}'.format(service_account_id)
    if description:
        cmd += ' --description "{}"'.format(description)
    return call_and_parse(cmd)

def delete_api_key(api_key):
    cmd = 'api-key delete ' + api_key
    call(cmd)

def update_api_key(api_key, description):
    """
    Update description of api-key.
    """
    cmd = 'api-key update {} --description "{}"'.format(api_key, description)
    call(cmd)

# functions for ACLs
def list_acl():
    cmd = 'kafka acl list'
    return call_and_parse(cmd)

def create_acl_entry(allow_or_deny, operations, service_account_id, resource_type, resource_name=None, is_prefix=None):
    """
    operations can be a string or a list of strings
    """
    valid_resource_types = ['cluster', 'consumer-group', 'topic', 'transactional-id']
    valid_operations = ['alter', 'alter-configs', 'cluster-action', 'create', 'delete', 'describe', 'describe-configs', 'idempotent-write', 'read', 'write']
    if isinstance(operations, str):
        operations = operations.split(',')
    operations = list(map( lambda s: s.strip().lower(), operations))
    if not set(operations).issubset(set(valid_operations)):
        msg = "Unknown operation in {}, valid values are {}".format(operations, valid_operations)
        raise Exception(msg)
    if not resource_type in valid_resource_types:
        msg = "Unknown resource type '{}'. Valid types are: {}".format(resource_type, valid_resource_types)
        raise Exception(msg)
    if resource_type != 'cluster' and not resource_name:
        msg = "A resource must be specified for resouce type {}".format(resource_type)
        raise Expection(msg)
    resource_formatted = resource_type + ' ' + resource_name if resource_name else ''

    if not allow_or_deny in ['allow', 'deny']:
        raise Exception('need to specify allow/deny')
    cmd = 'kafka acl create --{} --service-account {} --{} {}'.format(
        allow_or_deny,
        service_account_id,
        resource_formatted,
        " ".join('--operation ' + op for op in operations)
    )
    if is_prefix:
        cmd += " --prefix"
    call(cmd)

def delete_acl_entry():
    pass
