FROM eoscprominence/htcondor:8.9.2

RUN yum -y install epel-release && \
    yum -y install python-pip \
                   python-devel \
                   gcc \
                   openssh-clients && \
    pip install uwsgi \
                flask \
                requests \
                boto3 \
                PyJWT \
                elasticsearch \
                elasticsearch-dsl && \
    useradd prominence
                   
COPY restapi.py /usr/local/bin/
COPY backend.py /usr/local/bin/
COPY validate.py /usr/local/bin/
COPY retry.py /usr/local/bin/
COPY data.py /usr/local/bin/
COPY auth.py /usr/local/bin/
COPY jobs.py /usr/local/bin/
COPY workflows.py /usr/local/bin/
COPY errors.py /usr/local/bin/
COPY utilities.py /usr/local/bin/
COPY usage.py /usr/local/bin/
COPY accounting.py /usr/local/bin/
COPY promlet.py /usr/local/libexec/

ENTRYPOINT ["uwsgi", \
            "--http-socket", "localhost:8080", \
            "--threads", "4", \
            "--close-on-exec", \
            "--uid", "prominence", \
            "--gid", "prominence", \
            "--manage-script-name", \
            "--master", \
            "--chdir", "/usr/local/bin", \
            "--mount", "/usr/local/bin=restapi:app"]
