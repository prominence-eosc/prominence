#!/usr/bin/python

import htcondor
import classad
import time

coll = htcondor.Collector()

sites = []
identities = []

jobs_by_identity_r = {}
cpus_by_identity_r = {}
jobs_by_identity_i = {}
cpus_by_identity_i = {}
jobs_by_identity_site_r = {}
cpus_by_identity_site_r = {}

results = coll.query(htcondor.AdTypes.Schedd, "true", ["Name"])
for result in results:
    host = result["Name"]
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, host)
    schedd = htcondor.Schedd(schedd_ad)
    jobs = schedd.query('RoutedBy =!= "jobrouter" && Cmd != "/usr/bin/condor_dagman"',
                        ["JobStatus",
                         "ProminenceInfrastructureSite",
                         "ProminenceIdentity",
                         "RequestCpus"])
    for job in jobs:
        if "JobStatus" in job and "ProminenceIdentity" in job:
            identity = job['ProminenceIdentity']

            if identity not in identities:
                identities.append(identity)

            site = None
            if "ProminenceInfrastructureSite" in job:
                site = job["ProminenceInfrastructureSite"]

            if identity not in jobs_by_identity_i:
                jobs_by_identity_i[identity] = 0

            if identity not in jobs_by_identity_r:
                jobs_by_identity_r[identity] = 0

            if identity not in cpus_by_identity_r:
                cpus_by_identity_r[identity] = 0
                cpus_by_identity_i[identity] = 0

            if identity not in jobs_by_identity_site_r:
                jobs_by_identity_site_r[identity] = {}
                jobs_by_identity_site_r[identity][site] = 0

            if identity not in cpus_by_identity_site_r:
                cpus_by_identity_site_r[identity] = {}
                cpus_by_identity_site_r[identity][site] = 0
            
            if job["JobStatus"] == 1:
                jobs_by_identity_i[identity] += 1
                cpus_by_identity_i[identity] += int(job["RequestCpus"])

            if job["JobStatus"] == 2:
                jobs_by_identity_r[identity] += 1
                cpus_by_identity_r[identity] += int(job["RequestCpus"])
                jobs_by_identity_site_r[identity][site] += 1
                cpus_by_identity_site_r[identity][site] += int(job["RequestCpus"])

                if site and site not in sites:
                    sites.append(site)
              
for identity in identities:
    print("jobs_by_identity,identity=%s idle=%d,running=%d" % (identity, jobs_by_identity_i[identity], jobs_by_identity_r[identity]))
    print("cpus_by_identity,identity=%s idle=%d,running=%d" % (identity, cpus_by_identity_i[identity], cpus_by_identity_r[identity]))
    for site in sites:
        if site in jobs_by_identity_site_r[identity]:
            print("jobs_by_identity_by_site,identity=%s,site=%s running=%d" % (identity, site, jobs_by_identity_site_r[identity][site]))
            print("cpus_by_identity_by_site,identity=%s,site=%s running=%d" % (identity, site, cpus_by_identity_site_r[identity][site]))

