import json
import logging
import os
import re
import signal
import socket
import sys
import time
import django

from django.core.management.base import BaseCommand
from django.db.models import Q

from frontend.models import Job, Workflow
import server.settings

from update_db import check_db

logger = logging.getLogger('metrics')

class Command(BaseCommand):

    def create_metrics(self):
        jobs_by_identity_pending = {}
        jobs_by_identity_running = {}
        jobs_by_identity_site_running = {}
        cpus_by_identity_pending = {}
        cpus_by_identity_running = {}
        cpus_by_identity_site_running = {}
        identities = []
        sites = []

        jobs = Job.objects.filter(Q(status=0) | Q(status=1))
        for job in jobs:
            if job.user.username not in identities:
                identities.append(job.user.username)

            if job.user.username not in jobs_by_identity_pending:
                jobs_by_identity_pending[job.user.username] = 0
                cpus_by_identity_pending[job.user.username] = 0

            if job.user.username not in jobs_by_identity_running:
                jobs_by_identity_running[job.user.username] = 0
                cpus_by_identity_running[job.user.username] = 0

            jobs_by_identity_pending[job.user.username] += 1
            cpus_by_identity_pending[job.user.username] += job.request_cpus

        jobs = Job.objects.filter(Q(status=2))
        for job in jobs:
            if job.user.username not in identities:
                identities.append(job.user.username)

            if job.user.username not in jobs_by_identity_pending:
                jobs_by_identity_pending[job.user.username] = 0
                cpus_by_identity_pending[job.user.username] = 0

            if job.user.username not in jobs_by_identity_running:
                jobs_by_identity_running[job.user.username] = 0
                cpus_by_identity_running[job.user.username] = 0

            jobs_by_identity_running[job.user.username] += 1
            cpus_by_identity_running[job.user.username] += job.request_cpus

            if job.site:
                if job.site not in sites:
                    sites.append(job.site)
                if job.user.username not in cpus_by_identity_site_running:
                    cpus_by_identity_site_running[job.user.username] = {}
                    jobs_by_identity_site_running[job.user.username] = {}
                if job.site not in cpus_by_identity_site_running[job.user.username]:
                    cpus_by_identity_site_running[job.user.username][job.site] = 0
                    jobs_by_identity_site_running[job.user.username][job.site] = 0

                jobs_by_identity_site_running[job.user.username][job.site] += 1
                cpus_by_identity_site_running[job.user.username][job.site] += job.request_cpus

        # Create messages
        messages = []
        for identity in identities:
            messages.append('jobs_by_identity,identity=%s idle=%d,running=%d\n' % (identity, jobs_by_identity_pending[identity], jobs_by_identity_running[identity]))
            messages.append('cpus_by_identity,identity=%s idle=%d,running=%d\n' % (identity, cpus_by_identity_pending[identity], cpus_by_identity_running[identity]))

            for site in sites:
                if site in cpus_by_identity_site_running[identity]:
                    messages.append('jobs_by_identity_by_site,identity=%s,site=%s running=%d\n' % (identity, site, jobs_by_identity_site_running[identity][site]))
                    messages.append('cpus_by_identity_by_site,identity=%s,site=%s running=%d\n' % (identity, site, cpus_by_identity_site_running[identity][site]))

        # Send messages to Telegraf
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
                s.connect('/tmp/telegraf.sock')
                for message in messages:
                    s.send(message.encode('utf8'))
        except Exception as err:
            logger.critical('Unable to send metrics due to: %s', err)

    def handle(self, **options):

        while True:
            if not check_db():
                sys.exit(1)

            self.create_metrics()

            time.sleep(60)
