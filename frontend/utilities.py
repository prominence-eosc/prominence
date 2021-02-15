import base64

def create_job(data, data_envvars, data_labels, files, data_artifacts, data_output_files, data_output_dirs, storage_list, uuid):
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

    if 'policy_leave_job_in_queue' in data:
        if data['policy_leave_job_in_queue']:
            policies['leaveInQueue'] = True

    if 'policy_job_max_time_pending' in data:
        if data['policy_job_max_time_pending'] > 0:
            policies['maximumTimeInQueue'] = 60*data['policy_job_max_time_pending']

    if 'policy_sites' in data:
        if data['policy_sites'] != "":
            policies['placement'] = {}
            policies['placement']['requirements'] = {}
            policies['placement']['requirements']['sites'] = data['policy_sites'].split(',')

            if ',' in data['policy_sites']:
                policies['placement']['preferences'] = {}
                policies['placement']['preferences']['sites'] = data['policy_sites'].split(',')

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
        resources['nodes'] = data['nodes']
    job['resources'] = resources

    notifications = []
    if 'notify_email_job_finished' in data:
        if data['notify_email_job_finished']:
            notify = {}
            notify['event'] = 'jobFinished'
            notify['type'] = 'email'
            notifications.append(notify)

    if notifications:
        job['notifications'] = notifications

    if 'storage_name' in data:
        for storage in storage_list:
            if str(data['storage_name']) == str(storage.name):
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
                    job_storage['onedata']['token'] = storage.token
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

    output_files = []
    for output_file in data_output_files:
        output_file_c = output_file.cleaned_data
        if output_file_c:
            output_files.append(output_file_c.get('name'))
    if output_files:
        job['outputFiles'] = output_files

    output_dirs = []
    for output_dir in data_output_dirs:
        output_dir_c = output_dir.cleaned_data
        if output_dir_c:
            output_dirs.append(output_dir_c.get('name'))
    if output_dirs:
        job['outputDirs'] = output_dirs

    inputs = []
    for input_file in files:
        new_input = {}
        new_input['filename'] = files[input_file].name
        new_input['content'] = base64.b64encode(files[input_file].read()).decode("utf-8")
        inputs.append(new_input)

    if inputs:
        job['inputs'] = inputs

    return job
