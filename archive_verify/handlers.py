import json
import logging

from aiohttp import web
from redis import Redis
from rq import Queue

from archive_verify.workers import verify_archive

log = logging.getLogger(__name__)

# TODO: Fix better comments
# TODO: Fix better error handling
# TODO: Fix better return values so it fits with the workflow
# TODO: Fix better logging
# TODO: Add test cases

def dummy_job(archive, description, config):
    import time
    print("Starting download of archive {} with description {}".format(archive, description))
    time.sleep(60)
    print("Finishing downloading archive.")
    return True

async def verify(request):
    """
    POST verify/
    BODY: runfolder + descr + host
    RETURN: Id of enqueued job.
    """
    body = await request.json()
    archive = body["archive"]
    description = body["description"]
    host = body["host"]

    redis_conn = Redis()
    q = Queue(connection=redis_conn)
    job = q.enqueue_call(verify_archive, #dummy_job
                        args=(archive, host, description, request.app["config"]),
                        timeout=request.app["config"]["job_timeout"],
                        result_ttl=request.app["config"]["job_result_ttl"],
                        ttl=request.app["config"]["job_ttl"])

    status_end_point = "{0}://{1}{2}".format(request.scheme, request.host, rel_url("status", job.id))

    response = { "status": "pending", "job_id": job.id, "link": status_end_point }
    return web.json_response(response)

async def status(request):
    """
    GET status/<id>
    Check status of job in queue
    """
    log.info("Get call to /status")

    job_id = str(request.match_info['job_id'])

    redis_conn = Redis()
    q = Queue(connection=redis_conn)
    job = q.fetch_job(job_id)

    if job:
        if job.is_started:
            payload = {"state": "started", "msg": "Job {} is currently running.".format(job_id)}
            code = 200
        elif job.is_finished:
            result = job.result

            if result and result["state"] == "done": 
                payload =  {"state": "done", "msg": "Job {} has returned with result: {}".format(job_id, job.result)}
                code = 200
            else: 
                payload =  {"state": "done", "msg": "Job {} has returned with result: {}".format(job_id, job.result)}
                code = 500

            job.delete()
        elif job.is_failed:
            payload = {"state": "error", "msg": "Job {} failed with error: {}".format(job_id, job.exc_info)}
            job.delete()
            code = 500
        else:
            payload = {"state": "pending", "msg": "Job {} has not started yet.".format(job_id)}
            code = 200
    else:
        payload = {"state": "error", "msg": "No such job {} found!".format(job_id)}
        code = 400

    return web.json_response(payload, status=code)
