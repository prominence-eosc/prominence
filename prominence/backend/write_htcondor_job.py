# Job template
JOB_SUBMIT = \
"""
universe = %(universe)s
executable = promlet.py
arguments = --job $(mappedjson) --id $(prominencecount) %(extra_args)s
output = job.$(prominencecount).out
error = job.$(prominencecount).err
log = job.$(prominencecount).log
should_transfer_files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
transfer_output_files = logs,json
skip_filechecks = true
requirements = %(requirements)s
transfer_executable = true
stream_output = true
stream_error = true
RequestCpus = %(cpus)s
RequestMemory = %(reqmemory)s
RequestDisk = %(reqdisk)s
accounting_group_user = %(accounting_group_user)s
leave_in_queue = %(leave_in_queue)s
+ProminenceRemoveFromQueue = %(remove_from_queue)s
+ProminenceAPI = %(apivers)s
+ProminenceJobUniqueIdentifier = %(uuid)s
+ProminenceIdentity = %(username)s
+ProminenceGroup = %(group)s
+ProminenceName = %(name)s
+ProminenceEmail = %(email)s
+ProminenceMaxIdleTime = %(maxidle)s
+ProminenceMaxTimeInQueue = %(maxtimeinqueue)s
+ProminenceMaxRunTime = %(maxruntime)s
+ProminenceWantMPI = %(wantmpi)s
+ProminenceType = "job"
+ProminenceJobToken = %(jobtoken)s
+ProminenceURL = %(joburl)s
%(extras)s
%(extras_metadata)s
queue 1
"""

def write_htcondor_job(cjob, filename):
    """
    Write a HTCondor JDL
    """
    keys = ['transfer_input_files',
            '+remote_cerequirements_default',
            '+ProminenceFactoryId',
            '+ProminenceWorkflowName',
            'Rank',
            '+JobPrio',
            '+ProminenceDynamicMPI',
            '+WantParallelSchedulingGroups',
            '+ProminenceAutoScalingType',
            'machine_count',
            'max_retries',
            'accounting_group']
    extras = "\n"
    for key in keys:
        if key in cjob:
            extras += "%s = %s\n" % (key, cjob[key])

    info = {}
    info['name'] = cjob['+ProminenceName']
    info['uuid'] = cjob['+ProminenceJobUniqueIdentifier']
    info['username'] = cjob['+ProminenceIdentity']
    info['group'] = cjob['+ProminenceGroup']
    info['accounting_group_user'] = cjob['accounting_group_user']
    info['reqmemory'] = cjob['RequestMemory']
    info['reqdisk'] = cjob['RequestDisk']
    info['cpus'] = cjob['RequestCpus']
    info['wantmpi'] = cjob['+ProminenceWantMPI']
    info['maxidle'] = 0
    info['maxtimeinqueue'] = cjob['+ProminenceMaxTimeInQueue']
    info['extras'] = extras
    info['maxruntime'] = cjob['+ProminenceMaxRunTime']
    info['email'] = cjob['+ProminenceEmail']
    if 'extra_args' in cjob:
        info['extra_args'] = cjob['extra_args']
    else:
        info['extra_args'] = ''
    info['requirements'] = cjob['Requirements']
    info['jobtoken'] = cjob['+ProminenceJobToken']
    info['joburl'] = cjob['+ProminenceURL']
    info['universe'] = cjob['universe']
    info['apivers'] = cjob['+ProminenceAPI']
    info['leave_in_queue'] = cjob['leave_in_queue']
    info['remove_from_queue'] = cjob['+ProminenceRemoveFromQueue']

    # Add any labels
    extras_metadata = ''
    for item in cjob:
        if 'ProminenceUserMetadata' in item:
            extras_metadata += '%s = %s\n' % (item, cjob[item])
    info['extras_metadata'] = extras_metadata

    # Write to a file
    try:
        with open(filename, 'w') as fd:
            fd.write(JOB_SUBMIT % info)
    except IOError:
        return False

    return True
