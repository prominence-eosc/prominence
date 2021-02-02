import sys
from update_db import update_job_in_db

if __name__ == "__main__":
    condor_job_id = int(sys.argv[1])
    start_time = int(sys.argv[2])
    site = sys.argv[3]
    reason = int(sys.argv[4])

    status = 2

    if start_time == 0:
        start_time = None
        status = 1

    update_job_in_db(condor_job_id, status, start_date=start_time, site=site, reason=reason)
