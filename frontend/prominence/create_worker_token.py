import subprocess

def create_condor_worker_token(user):
    """
    Create token for HTCondor auth
    """
    if '@' not in user:
        user = '%s@cloud' % user

    run = subprocess.run(["sudo",
                          "condor_token_create",
                          "-identity",
                          user,
                          "-key",
                          "token_key",
                          "-authz",
                          "ADVERTISE_STARTD",
                          "-authz",
                          "ADVERTISE_MASTER"])

    if run.returncode == 0:
        return run.stdout
    else:
        return None
