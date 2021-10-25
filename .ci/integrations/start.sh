#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

SRC_DIR=$(git rev-parse --show-toplevel)
cd $SRC_DIR

IMAGE_NAME=pulsar-io-cloud-storage-test:latest
MVN_VERSION=`${SRC_DIR}/.ci/versions/get-project-version.py`

docker build -t ${IMAGE_NAME} .

docker network create cloud-storage-test

docker kill pulsar-io-cloud-storage-test || true
docker run  --network cloud-storage-test -d --rm --name pulsar-io-cloud-storage-test \
            -p 8080:8080 \
            -p 6650:6650 \
            -p 8443:8843 \
            -p 6651:6651 \
            ${IMAGE_NAME} \
            /pulsar/bin/pulsar standalone \
            --no-stream-storage

PULSAR_ADMIN="docker exec -d pulsar-io-cloud-storage-test /pulsar/bin/pulsar-admin"

echo "-- Wait for Pulsar service to be ready"
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done

echo "-- Pulsar service ready to test"

docker kill localstack || true
docker run  --network cloud-storage-test -d --rm --name localstack \
            -p 4566:4566 \
            -e SERVICES=s3 \
            -e DEFAULT_REGION=us-east-1 \
            -e HOSTNAME_EXTERNAL=localstack \
            localstack/localstack:latest

sudo echo "127.0.0.1 localstack" | sudo tee -a /etc/hosts
echo "-- Wait for localstack service to be ready"
until $(curl --silent --fail http://localstack:4566/health | grep "\"s3\": \"running\"" > /dev/null);  do sleep 1; done
echo "-- localstack service ready to test"

docker ps

# run connector
echo "-- run pulsar-io-cloud-storage sink connector"
$PULSAR_ADMIN sources localrun -a /pulsar-io-cloud-storage/target/pulsar-io-cloud-storage-${MVN_VERSION}.nar \
        --tenant public --namespace default --name test-cloud-storage-sink \
        --source-config-file /pulsar-io-cloud-storage/.ci/test-pulsar-io-cloud-storage-sink-s3.yaml \
        --destination-topic-name test-cloud-storage-sink-topic

echo "-- ready to do integration tests"