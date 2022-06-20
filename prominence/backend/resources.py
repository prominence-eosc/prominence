import htcondor
import classad
import time

def get_existing_resources(self, group):
    """
    Get list of worker nodes
    """
    try:
        coll = htcondor.Collector()

        results = coll.query(htcondor.AdTypes.Startd,
                             'PartitionableSlot=?=True',
                             ["TotalCpus", "Cpus", "TotalMemory", "Memory", "TotalDisk", "ProminenceCloud", "Start"])
    except:
        return None

    workers = []
    for result in results:
        if group in str(result['Start']) or 'ProminenceGroup' not in str(result['Start']):
            capacity = {'cpus': int(result["TotalCpus"]), 'memory': int(result["TotalMemory"]/1024.0)}
            free = {'cpus': int(result["Cpus"]), 'memory': int(result["Memory"]/1024.0)}
            worker = {'capacity': capacity, 'free': free, 'site': result["ProminenceCloud"]}
            workers.append(worker)

    # Sort by free CPUs descending
    workers = sorted(workers, key=lambda x: x['free']['cpus'], reverse=True)

    data = {'existing': workers}
    
    return data
