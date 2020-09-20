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
    jobs = schedd.query('RoutedBy == "jobrouter" && JobStatus == 2',
                        ["ResidentSetSize_RAW", "RoutedFromJobId"])

    for job in jobs:
        if "RoutedFromJobId" in job and "ResidentSetSize_RAW" in job:
            job_id = job["RoutedFromJobId"].split('.')[0]
            print("jobs_resource_usage,id=%d memory=%d" % (int(job_id), int(job["ResidentSetSize_RAW"])))

