Arteria Archive Verify
==================

A self contained (aiohttp) REST service that helps verify uploaded SNP&SEQ archives by first downloading the archive from PDC, and then compare the MD5 sums for all associated files.  

Trying it out
-------------

    python3 -m pip install pipenv
    pipenv install --deploy


Try running it:

     pipenv run ./archive-verify-ws -c=config/

Running tests
-------------

    pipenv install --dev
    pipenv run nosetests tests/


REST endpoints
--------------

# FIXME: Update example
