#!/bin/bash

# This script creates a zip file to be used as a Lambda layer with Python requirements.
# It uses docker to make sure the installed packages are compatible with
# the Lambda runtime.
# It also temporarily copies the local packages from the shared_packages folder to
# the current dir. For more info about this process, see:
# https://postnl.atlassian.net/wiki/spaces/ESB/pages/2947613028/Code+Reuse+through+packages

# Remove the python dir if it exists
rm -rf python | true
# Remove the python zip file if it exists
rm -rf python.zip | true
# Run docker to install the packages compatible with the Lambda Linux OS
#docker run -it -v "$PWD":/var/task "lambci/lambda:build-python3.8" /bin/sh -c "pip install -r requirements.txt -t python/lib/python3.8/site-packages/; exit"
docker run -it -v "$PWD":/var/task "mlupin/docker-lambda:python3.9-build" /bin/sh -c "pip install -r requirements.txt -t python/lib/python3.9/site-packages/; exit"
# Compress the result into a zip file
zip -r python.zip python > /dev/null;
# Remove the python dir again, we don't need it anymore
rm -rf python | true
