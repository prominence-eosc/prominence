def create_job(data):
    """
    Create JSON description of job from form
    """
    job = {}

    if 'name' in data:
        job['name'] = data['name']
    
    task = {}
    if 'container_image' in data:
        task['runtime'] = data['container_image']
    if 'command' in data:
        task['cmd'] = data['command']
    if 'container_image' in data:
        task['image'] = data['container_image']

    job['tasks'] = [task]

    resources = {}
    if 'cpus' in data:
        resources['cpus'] = data['cpus']
        resources['memory'] = data['memory']
        resources['disk'] = data['disk']
    job['resources'] = resources

    return job
