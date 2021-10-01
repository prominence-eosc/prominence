FROM alahiff/htcondor:9.2.0

RUN yum -y install epel-release && \
    yum -y install python3-pip \
                   python3-devel \
                   gcc \
                   openssh-clients && \
    useradd prominence --uid 1005

RUN mkdir /tmp/prominence
COPY setup.py /tmp/prominence/.
COPY README.md /tmp/prominence/.
COPY prominence /tmp/prominence/prominence/
COPY bin /tmp/prominence/bin/

RUN cd /tmp/prominence && \
    pip3 install . && \
    rm -rf /tmp/prominence

COPY promlet.py /usr/local/libexec/

ENTRYPOINT ["uwsgi", \
            "--http-socket", "localhost:8080", \
            "--processes", "4", \
            "--enable-threads", \
            "--close-on-exec", \
            "--uid", "prominence", \
            "--gid", "prominence", \
            "--manage-script-name", \
            "--master", \
            "--chdir", "/usr/local/bin", \
            "-w", "prominence-restapi:app"]
