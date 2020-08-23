# Job template
JOB_SUBMIT = \
"""
universe = vanilla
executable = promlet.py
arguments = --job .job.mapped.json --id $(prominencecount) %(extra_args)s
output = job.$(prominencecount).out
error = job.$(prominencecount).err
log = job.$(prominencecount).log
should_transfer_files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
transfer_output_files = promlet.$(prominencecount).log,promlet.$(prominencecount).json
skip_filechecks = true
requirements = false
transfer_executable = true
stream_output = true
stream_error = true
RequestCpus = %(cpus)s
RequestMemory = %(reqmemory)s
+ProminenceJobUniqueIdentifier = %(uuid)s
+ProminenceIdentity = %(username)s
+ProminenceGroup = %(group)s
+ProminenceName = %(name)s
+ProminenceMaxIdleTime = %(maxidle)s
+ProminenceMaxTimeInQueue = %(maxtimeinqueue)s
+ProminenceWantMPI = %(wantmpi)s
+ProminenceType = "job"
+WantIOProxy = true
%(extras)s
%(extras_metadata)s
queue 1
"""

def write_htcondor_job(cjob, filename):
    """
    Write a HTCondor JDL
    """
    keys = ['transfer_input_files',
            '+ProminenceWantJobRouter',
            '+remote_cerequirements_default',
            '+ProminenceFactoryId',
            '+ProminenceWorkflowName']
    extras = "\n"
    for key in keys:
        if key in cjob:
            extras += "%s = %s\n" % (key, cjob[key])

    info = {}
    info['name'] = cjob['+ProminenceName']
    info['uuid'] = cjob['+ProminenceJobUniqueIdentifier']
    info['username'] = cjob['+ProminenceIdentity']
    info['group'] = cjob['+ProminenceGroup']
    info['reqmemory'] = cjob['RequestMemory']
    info['cpus'] = cjob['RequestCpus']
    info['wantmpi'] = cjob['+ProminenceWantMPI']
    info['maxidle'] = 0
    info['maxtimeinqueue'] = cjob['+ProminenceMaxTimeInQueue']
    info['extras'] = extras
    if 'extra_args' in cjob:
        info['extra_args'] = cjob['extra_args']
    else:
        info['extra_args'] = ''

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
