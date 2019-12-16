import os

def _output_urls(self, workflow, uid, label):
    """
    Generate output files/dirs
    """
    lists = ''
    count = 0

    for job in workflow['jobs']:
        if 'outputFiles' in job:
            for filename in job['outputFiles']:
                url_put = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    'scratch/%s/%d/%s' % (uid, label, os.path.basename(filename)),
                                                    604800)
                lists = lists + ' prominenceout%d="%s" ' % (count, url_put)
                count += 1

        if 'outputDirs' in job:
            for dirname in job['outputDirs']:
                url_put = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    'scratch/%s/%d/%s.tgz' % (uid, label, os.path.basename(dirname)),
                                                    604800)
                lists = lists + ' prominenceout%d="%s" ' % (count, url_put)
                count += 1

    return lists
