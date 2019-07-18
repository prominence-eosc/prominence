FROM eoscprominence/htcondor:8.9.2

COPY restapi.py /usr/local/bin/
COPY backend.py /usr/local/bin/
COPY validate.py /usr/local/bin/
COPY retry.py /usr/local/bin/
COPY promlet.py /usr/local/libexec/

RUN yum -y install epel-release && \
    yum -y install python-pip \
                   python-devel \
                   gcc && \
    pip install uwsgi \
                flask \
                requests \
                boto3 && \
    chmod a+xr /usr/local/libexec/promlet.py && \
    useradd prominence
                   
ENTRYPOINT ["uwsgi", \
            "--http-socket", ":8080", \
            "--threads", "2", \
            "--uid", "prominence", \
            "--gid", "prominence", \
            "--manage-script-name", \
            "--master", \
            "--chdir", "/usr/local/bin", \
            "--mount", "/usr/local/bin=restapi:app"]
