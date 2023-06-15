#!/usr/bin/python
from google.cloud import bigquery
from dateutil import tz
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta


class Job:
    def __init__(self, name, query=None, config=None, job_id=None, created=None, last_state=None, last_checked=None):
        self.name = name
        self.query = query
        self.config = config
        self.job_id = job_id
        self.created = created
        self.last_state = last_state
        self.last_checked = last_checked
        self.error = None
    
    def __repr__(self):
        return self.name
    
    def __str__(self):
        created_string = f"Created: {self.created.astimezone(tz.tzlocal()).strftime('%b %d, %Y %I:%M %p')}" if self.created else None
        last_checked_string = f"Last Checked: {self.last_checked.astimezone(tz.tzlocal()).strftime('%b %d, %Y %I:%M %p')}" if self.last_state else None
        if self.error:
            return "\t | \t".join([self.name, self.job_id, created_string, last_checked_string, self.error['message']])
        else:
            return "\t | \t".join([self.name, self.job_id, created_string, last_checked_string, self.last_state])
            
class QueryTool:
    def __init__(self, jobs=list()):
        self.jobs = jobs
    
    def __repr__(self):
        return jobs
    
    def multithread(self, fn, jobs):
        if not jobs:
            jobs = self.jobs
        # Submitting queries is I/O bound
        with ThreadPoolExecutor(len(jobs)) as executor:
            # create client and send to function
            threads = [executor.submit(fn, job) for job in jobs]
            for future in as_completed(threads):
                future.result()
            
    def run(self, job):
        with bigquery.Client() as client:
            query_results = client.query(job.query, job_config=job.config)
        job.job_id = query_results.job_id
        
        job.created = query_results.created
        return job.name, job.job_id
         
    def build(self, base_query, variants):
        for name, params in variants.items():
            query = base_query.format(report=name)
            config = bigquery.QueryJobConfig(query_parameters = [bigquery.ScalarQueryParameter(k, "STRING", v) for k,v in params.items()], priority = bigquery.QueryPriority.BATCH)
            self.jobs.append(Job(name, query, config))
        self.multithread(self.run, self.jobs)
        self.monitor(self.jobs)
        return self.jobs
    
    def get_status(self, job):
        with bigquery.Client() as client:
            job_status = client.get_job(job.job_id)
        job.last_state = job_status.state
        job.last_checked = datetime.now()
        job.error = job_status.error_result
        print(job)
            
    def monitor(self, jobs=None):
        if not jobs:
            jobs = self.jobs
        self.multithread(self.get_status, jobs)
    
    def cancel_job(self, job):
        with bigquery.Client() as client:
            job_status = client.cancel_job(job.job_id)
            print(f"{job.name}: {job_status.job_id} cancelled")
        job = None
    
    def cancel(self, jobs=None):
        if not jobs:
            jobs = self.jobs
        self.multithread(self.cancel_job, jobs)
        jobs = []