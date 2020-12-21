#!/usr/bin/python

import htcondor
import classad
import time

coll = htcondor.Collector()

results = coll.query(htcondor.AdTypes.Schedd, "true", ["Name"])
for result in results:
    host = result["Name"]
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, host)
    schedd = htcondor.Schedd(schedd_ad)
    jobs = schedd.query('JobStatus == 2 && isUndefined(RoutedToJobId) && Cmd != "/usr/bin/condor_dagman"',
                        ["ResidentSetSize_RAW", "RoutedFromJobId", "ClusterId"])

    for job in jobs:
        if "ResidentSetSize_RAW" in job:
            if "RoutedFromJobId" in job:
                job_id = job["RoutedFromJobId"].split('.')[0]
            else:
                job_id = job["ClusterId"]
            print("jobs_resource_usage,id=%d memory=%d" % (int(job_id), int(job["ResidentSetSize_RAW"])))

