import subprocess

def create_condor_worker_token(user):
    """
    Create token for HTCondor auth
    """
    if '@' not in user:
        user = '%s@cloud' % user

    process = subprocess.Popen(["sudo",
                          "condor_token_create",
                          "-identity",
                          user,
                          "-key",
                          "token_key",
                          "-authz",
                          "ADVERTISE_STARTD",
                          "-authz",
                          "ADVERTISE_MASTER"],
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        return stdout.strip().decode('utf-8')

    return None
