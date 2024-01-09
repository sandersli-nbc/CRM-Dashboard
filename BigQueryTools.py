#!/usr/bin/python
import re
from google.cloud import bigquery
from dateutil import tz
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from google.api_core.exceptions import NotFound
import time
from win10toast import ToastNotifier

class SimpleQueryJob:
    """
    A class that acts as a simplified "wrapper" for bigquery.QueryJob
    Creates client on-demand
    
    ...
    
    Attributes
    ----------
    name: str
        Name of the job. Acts as the identifier of the object and should be unique for each instance
    query: str, Optional
        SQL query string
    config: google.cloud.bigquery.job.QueryJobConfig
        Configuration options for the query job
    job_id: str, Optional
        The job's ID assigned by BigQuery
    created: datetime.datetime, Optional
        The start time of the query in BigQuery
    last_state: str, Optional
        The last state of the query
    last_checked: datetime.datetime, Optional
        The last time the query state was checked
    error: str, Optional
        The error code of the query. Only exists if an error code was thrown
    active: bool, Optional
        Boolean indicating if the query is active or not. Reflects `last_state`.
    finished: bool, Optional
        Boolean indicating if the query is finished successfully. Reflects `last_state` and `error`.
    """
    def __init__(self, name, query=None, config=None, job_id=None, created=None, last_state=None, last_checked=None):
        self.name = name
        self.query = query
        self.config = config
        self.job_id = job_id
        self.created = created
        self.last_state = last_state
        self.last_checked = last_checked
        self.error = None
        self.active = False
        self.finished = None
    
    def __repr__(self):
        return self.name
    
    def __str__(self):
        return self.status()
    
    def status(self):
        """
        Outputs the current status of the SimpleQueryJob
        
        Parameters
        ----------
        None
        
        Returns
        -------
        str
            ASCII-formatted string reflecting the current status of the SimpleQueryJob
        """
        fields = [self.name]
        if self.job_id:
            fields.append(self.job_id)
            fields.append(f"Created: {self.created.astimezone(tz.tzlocal()).strftime('%b %d, %Y %I:%M %p')}") if self.created else None
            fields.append(f"Last Checked: {self.last_checked.astimezone(tz.tzlocal()).strftime('%b %d, %Y %I:%M %p')}") if self.last_state else None
            if self.error:
                fields.append(self.error['message'])
            else:
                fields.append(self.last_state)
        else:
            fields.append('Awaiting start')
        return "\t | \t".join(fields)
    
    def start(self):
        """
        Starts the current job. Fails if query does not exist, or if job is active or finished.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        """
        if self.active or self.finished:
            print('Cannot start process, already active or finished')
            return
        with bigquery.Client() as client:
            results = client.query(self.query, job_config=self.config)
        self.job_id = results.job_id
        self.created = results.created
        self.active = True
        print(f'start {self.job_id}')
        
    def cancel(self):
        """
        Cancels the current job
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        """
        if self.active:
            with bigquery.Client() as client:
                status = client.cancel_job(self.job_id)
                print(f"{self.name}: {status.job_id} cancelled")
            self.active = False
            
    def update(self):
        """
        Checks with BigQuery to update the current state of the object
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        """
        if self.active:
            with bigquery.Client() as client:
                status = client.get_job(self.job_id)
            self.last_state = status.state
            self.last_checked = datetime.now()
            self.error = status.error_result
            if self.last_state == 'RUNNING':
                self.active = True
            else:
                self.active = False
                self.finished = status.ended          


class QueryTool:
    """
    A class to help manage queries asynchronously with on-demand client creation
    
    ...
    
    Attributes
    ----------
    jobs: dict
        dictionary of all queued or running jobs. Has structure: {<str name>: <class SimpleQueryJob>}
    finished: dict
        dictionary of all finished jobs. Has structure: {<str name>: <class SimpleQueryJob>}
    """
    def __init__(self, jobs=dict()):
        self.jobs = jobs
        self.finished_jobs = dict()
    
    def __repr__(self):
        active_jobs = "\n".join([job.status() for job in self.jobs.values()])
        finished_jobs = "\n".join([job.status() for job in self.finished_jobs.values()])
        return f'Active jobs: \n{active_jobs}\n\nFinished jobs: \n{finished_jobs}'
    
    def status(self):
        print(self)
        
    def _multithread(self, jobs, fn):
        if not jobs:
            jobs = self.jobs
        if not jobs:
            raise Exception('No Jobs Active')
        # Submitting queries is I/O bound
        with ThreadPoolExecutor(len(jobs)) as executor:
            # create client and send to function
            threads = [executor.submit(getattr(job, fn)) for job in jobs.values()]
            for future in as_completed(threads):
                future.result()
            
    def add(self, base_query: str, variants: str|dict, add_suffix: str=None):
        """
        Converts query to SimpleQueryJob(s) based on variants and adds to jobs
        
        Parameters
        ----------
        base_query: str
            Base query or formattable query to be converted to a SimpleQueryJob
        variant: str or dict
            Variants of base query to be created. 
            If str, creates single SimpleQueryJob with the variant string as the name
            If dict, creates multiple jobs. Expects the structure:
                {<str (name)> : {'params': <dict or QueryJobConfig (params)>, 'string_format': <dict (format)>}}
                Creates multiple SimpleQueryJobs based on <name> keys, each with :
                    params, a dict of structure {<str param_name>: <str param_value>} that is converted to a QueryJobConfig object with assumed 'STRING' type or a QueryJobConfig object
                    string_format, a dict formatting query string with keyword arguments
        add_suffix: str
            Appends str to end of all table names in the query. To be used for quick prototyping
        
        Returns
        -------
        None
        """
        #TODO: Detect type of query. Ensure create table exists. If formattable, require user to format. If configurable, require config file. If neither, default name to create table name
        if not isinstance(variants, str) and not isinstance(variants, dict):
            raise Exception('Please use either a string for naming single queries instances or a dictionary for configured/multiple instances')
        elif isinstance(variants, str):
            variants = {variants: {}}
        for name, values in variants.items():
            if name in self.jobs:
                if self.jobs[name].active:
                    print(f'Skipping {name}: Created and running')
                    continue
                else:
                    print(f'Replacing {name}')
            query = base_query.format(**values.get('string_format', {}))
            params = values.get('params')
            if add_suffix:
                query = re.sub(r"(`)([^\s`]+)(`)", r"\1_{}\2".format(add_suffix), query, flags=re.IGNORECASE)
            if isinstance(params, bigquery.QueryJobConfig):
                config = params
            elif params:
                config = bigquery.QueryJobConfig(query_parameters = [bigquery.ScalarQueryParameter(k, "STRING", v) for k,v in params.items()], priority = bigquery.QueryPriority.BATCH)
            else:
                config = None
            self.jobs[name] = SimpleQueryJob(name, query, config)
            if name in self.finished_jobs:
                del self.finished_jobs[name]
            print('Added:', self.jobs[name])
                           
    def start(self):
        """
        Iterates through jobs, starting them if they are not running
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        """
        queue = {}
        for name, job in self.jobs.items():
            if (job.active == True):
                print(f'Skipping {name}: Created and running')
                continue
            queue[name] = job
        self._multithread(queue, 'start')
        print('All jobs in queue started')
        
    def update(self, verbose=True):
        """
        Iterates through jobs, checking their status. If completed successfully, they are moved to the finished dict
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        """
        finished_jobs_keys = []
        self._multithread(self.jobs, 'update')
        for name, job in self.jobs.items():
            if (job.finished):
                finished_jobs_keys.append(name)
        for name in finished_jobs_keys:
            self.finished_jobs[name] = self.jobs.pop(name)
        if verbose:
            print(self)
    
    def await_finish(self):
        # create clients then use client.query.result() to wait. If Keyboard interrupt, close the clients
        while self.jobs:
            print('checking')
            self.update(verbose=False)
            for i in range(60):
                time.sleep(1)
        print(self)
        first_created = min([job.created for job in self.finished_jobs])
        last_finished = max([job.finished for job in self.finished_jobs])
        runtime = last_finished - first_created
        toaster = ToastNotifier()
        toaster.show_toast(f"All jobs finished. Run time: {'runtime'}")
    
    def await_finish_WIP(self):
        # create clients then use client.query.result() to wait. If Keyboard interrupt, close the clients
        with bigquery.Client() as client:
            for job in self.jobs:
                job = client.get_job(self.job_id)
                job.add_done_callback()
        print(self)
        first_created = min([job.created for job in self.finished_jobs])
        last_finished = max([job.finished for job in self.finished_jobs])
        runtime = last_finished - first_created
        toaster = ToastNotifier()
        toaster.show_toast(f"All jobs finished. Run time: {runtime}")
        
    def cancel(self):
        if self.jobs:
            self._multithread(self.jobs, 'cancel')
            print('Cleared all jobs')
        else:
            print('No jobs active')
        self.jobs = {}