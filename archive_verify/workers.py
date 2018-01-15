import logging
import subprocess
import os

from rq import get_current_job

log = logging.getLogger(__name__)

def _parse_dsmc_return_code(process, whitelist):
    log.debug("DSMC process returned an error!")

    # DSMC sets return code to 8 when a warning was encountered.
    if process.returncode == 8:
        log.debug("DSMC process actually returned a warning.")

        dsmc_out, _ = process.communicate()
        dsmc_out = dsmc_out.splitlines()

        # Search through the DSMC log and see if we only have
        # whitelisted warnings. If that is the case, change the
        # return code to 0 instead. Otherwise keep the error state.
        warnings = []

        for line in dsmc_out:
            matches = re.findall(r'ANS[0-9]+W', line)

            for match in matches:
                warnings.append(match)

        log.debug("Warnings found in DSMC output: {}".format(set(warnings)))

        for warning in warnings:
            if warning not in whitelist:
                log.debug("A non-whitelisted DSMC warning was encountered. Reporting it as an error!")
                return False

        log.debug("Only whitelisted DSMC warnings were encountered. Everything is OK.")
        return True
    else:
        log.info("An uncatched DSMC error code was encountered!")
        return False

# We do not need to have a file .pdc_description in the
# archive here I guess, as it will be this runfolder that
# is downloaded. But. In archive-remove it could be nice
# if we have a file there, so we are sure that it is
# the correct runfolder before we remove it.
def download_from_pdc(archive, description, dest, dsmc_log_dir, whitelist):
    cmd = "export DMS_LOG={} && dsmc retr {}/ {}/ -subdir=yes -description='{}'".format(dsmc_log_dir, archive, dest, description)
    #cmd = "echo {} {} > {}.txt".format(archive, description, dest)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    import time
    time.sleep(3)

    if p.returncode != 0:
        state = _parse_dsmc_return_code(p, whitelist)

        if state:
            return True
        else:
            return False

    return True

def compare_md5sum(root_dir):
    cmd = "md5sum -c {}/{}".format(root_dir, "checksums_prior_to_pdc.md5")
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    import time
    time.sleep(1)

    if p.returncode != 0: 
        return False
    else: 
        return True

def verify_archive(archive, host, description, config):
    dest_root = config["verify_root_dir"]
    job_id = get_current_job().id
    dest = "{}_{}".format(os.path.join(dest_root, archive), job_id)
    src_root = config["pdc_root_dir"].format(host)
    src = os.path.join(src_root, archive)
    dsmc_log_dir = config["dsmc_log_dir"]
    whitelist = config["whitelisted_warnings"]

    # TODO: More error checking
    download_ok = download_from_pdc(src, description, dest, dsmc_log_dir, whitelist)

    # dest is wrong
    # /data/mm-xart002/runfolders/johanhe_test_150821_M00485_0220_000000000-AG2UJ_archive_ca82ed0c-bc6a-4be7-9057-59fbc0a45411
    if not download_ok:
        return {"state": "error", "msg": "failed to properly download archive from pdc", "path": dest}
    else:
        verified_ok = compare_md5sum(dest)

        if verified_ok:
            return {"state": "done", "path": dest, "msg": "sucessfully verified archive's md5sums"}
        else:
            return {"state": "error", "path": dest, "msg": "failed to verify archive's md5sums"}
