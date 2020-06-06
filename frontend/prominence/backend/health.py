"""Return status of HTCondor daemons"""
import socket
import htcondor

def get_health(self):
    # Check collector
    try:
        coll = htcondor.Collector(socket.gethostname())
        if not coll.query(htcondor.AdTypes.Collector, 'true', ['Name']):
            return False
    except:
        return False

    # Check schedd
    try:
        coll = htcondor.Collector(socket.gethostname())
        schedds = coll.query(htcondor.AdTypes.Schedd, 'true', ['Name'])
        for schedd in schedds:
            schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, schedd['Name'])
            my_schedd = htcondor.Schedd(schedd_ad)
    except:
        return False

    return True
