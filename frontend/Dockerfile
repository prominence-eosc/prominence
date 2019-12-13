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

COPY prominence /prominence
COPY promlet.py /usr/local/libexec/

ENTRYPOINT ["uwsgi", \
            "--http-socket", "localhost:8080", \
            "--threads", "4", \
            "--close-on-exec", \
            "--uid", "prominence", \
            "--gid", "prominence", \
            "--manage-script-name", \
            "--master", \
            "--chdir", "/prominence", \
            "-w", "restapi:app"]
