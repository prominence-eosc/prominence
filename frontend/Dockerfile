FROM alahiff/htcondor:8.7.10

RUN yum -y install epel-release && \
    yum -y install python-pip \
                   python-devel \
                   gcc && \
    pip install uwsgi \
                flask \
                requests

COPY restapi.py /usr/local/bin/
COPY backend.py /usr/local/bin/

RUN useradd prominence

ENTRYPOINT ["uwsgi", \
            "--plugins", "http,python", \
            "--http-socket", ":8080", \
            "--threads", "2", \
            "--uid", "prominence", \
            "--manage-script-name", \
            "--master", \
            "--chdir", "/usr/local/bin", \
            "--mount", "/usr/local/bin=restapi:app"]
