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
COPY prominence-restapi.py /tmp/prominence/.

RUN pip3 install --upgrade pip

RUN cd /tmp/prominence && \
    pip3 install . && \
    rm -rf /tmp/prominence

# Executor
COPY promlet.py /usr/local/libexec/

# Allow prominence user to run condor_token_create
RUN yum -y install sudo && \
    echo "prominence ALL=(ALL:ALL) NOPASSWD:/usr/bin/condor_token_create" > /etc/sudoers.d/prominence && \
    chmod a-w /etc/sudoers.d/prominence && chmod o-r  /etc/sudoers.d/prominence

# Entrypoint
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
