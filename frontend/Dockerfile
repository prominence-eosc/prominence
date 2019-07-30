FROM eoscprominence/htcondor:8.9.2

RUN yum -y install epel-release && \
    yum -y install python-pip \
                   python-devel \
                   gcc && \
    pip install uwsgi \
                flask \
                requests \
                boto3 && \
    useradd prominence
                   
COPY restapi.py /usr/local/bin/
COPY backend.py /usr/local/bin/
COPY validate.py /usr/local/bin/
COPY retry.py /usr/local/bin/
COPY promlet.py /usr/local/libexec/

ENTRYPOINT ["uwsgi", \
            "--http-socket", "localhost:8080", \
            "--threads", "2", \
            "--uid", "prominence", \
            "--gid", "prominence", \
            "--manage-script-name", \
            "--master", \
            "--chdir", "/usr/local/bin", \
            "--mount", "/usr/local/bin=restapi:app"]
