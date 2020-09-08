import base64

def create_job(data, data_envvars, data_labels, files, data_artifacts, storage_list, uuid):
    """
    Create JSON description of job from form
    """
    job = {}

    if 'name' in data:
        job['name'] = data['name']
    
    task = {}
    if 'container_runtime' in data:
        task['runtime'] = data['container_runtime']
    if 'command' in data:
        if data['command'] != "":
            task['cmd'] = data['command']
    if 'container_image' in data:
        task['image'] = data['container_image']
    if 'workdir' in data:
        if data['workdir'] != "":
            task['workdir'] = data['workdir']

    policies = {}
    if 'policy_task_maxretries' in data:
        if data['policy_task_maxretries'] > 0:
            policies['maximumRetries'] = data['policy_task_maxretries']

    if policies:
        job['policies'] = policies

    env = {}
    for envvar in data_envvars:
        cenvvar = envvar.cleaned_data
        if cenvvar.get('key') and cenvvar.get('value'):
            env[cenvvar.get('key')] = cenvvar.get('value')
    if env:
        task['env'] = env

    job['tasks'] = [task]

    resources = {}
    if 'cpus' in data:
        resources['cpus'] = data['cpus']
        resources['memory'] = data['memory']
        resources['disk'] = data['disk']
        resources['walltime'] = data['walltime']*60 # convert hours to mins
    job['resources'] = resources

    if 'storage_name' in data:
        for storage in storage_list:
            if data['storage_name'] == storage.name:
                job_storage = {}
                job_storage['mountpoint'] = data['storage_mountpoint']
                if storage.storage_type == 1:
                    job_storage['type'] = 'webdav'
                    job_storage['webdav'] = {}
                    job_storage['webdav']['url'] = storage.hostname
                    job_storage['webdav']['username'] = storage.username
                    job_storage['webdav']['password'] = storage.password
                else:
                    job_storage['type'] = 'onedata'
                    job_storage['onedata'] = {}
                    job_storage['onedata']['provider'] = storage.hostname
                    job_storage['onedata']['token'] = storage.password
                job['storage'] = job_storage

    labels = {}
    for label in data_labels:
        clabel = label.cleaned_data
        if clabel.get('key') and clabel.get('value'):
            labels[clabel.get('key')] = clabel.get('value')
    if labels:
        job['labels'] = labels

    artifacts = []
    for artifact in data_artifacts:
        cartifact = artifact.cleaned_data
        if cartifact.get('url'):
            new_artifact = {}
            new_artifact['url'] = cartifact.get('url')
            if cartifact.get('executable'):
                new_artifact['executable'] = cartifact.get('executable')
            artifacts.append(new_artifact)
    if artifacts:
        job['artifacts'] = artifacts

    inputs = []
    for input_file in files:
        new_input = {}
        new_input['filename'] = files[input_file].name
        new_input['content'] = base64.b64encode(files[input_file].read()).decode("utf-8")
        inputs.append(new_input)

    if inputs:
        job['inputs'] = inputs

    return job
