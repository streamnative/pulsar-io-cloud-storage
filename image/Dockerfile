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

ARG PULSAR_VERSION
FROM streamnative/pulsar-functions-java-runner:${PULSAR_VERSION}

# Install the fixed PSU even when PULSAR_VERSION selects an older runner image.
USER 0
RUN set -eu; \
    JAVA_MAJOR=$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^[[:space:]]*java.specification.version = //p'); \
    case "$JAVA_MAJOR" in \
      17) CORRETTO_PACKAGE='amazon-corretto-17>=17.0.21.1.2-r0' ;; \
      21) CORRETTO_PACKAGE='amazon-corretto-21>=21.0.13.1.2-r0' ;; \
      *) echo "Unsupported runner Java major: $JAVA_MAJOR" >&2; exit 1 ;; \
    esac; \
    wget -q -O /etc/apk/keys/amazoncorretto.rsa.pub https://apk.corretto.aws/amazoncorretto.rsa.pub; \
    echo '6cfdf08be09f32ca298e2d5bd4a359ee2b275765c09b56d514624bf831eafb91  /etc/apk/keys/amazoncorretto.rsa.pub' | sha256sum -c -; \
    echo 'https://apk.corretto.aws' >> /etc/apk/repositories; \
    OLD_JAVA_PACKAGES=$(apk info | grep -E '^openjdk(17|21)(-|$)' || true); \
    if [ -n "$OLD_JAVA_PACKAGES" ]; then apk del $OLD_JAVA_PACKAGES; fi; \
    apk add --no-cache --upgrade "$CORRETTO_PACKAGE"; \
    /usr/lib/jvm/default-jvm/bin/java -version
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER 10000

USER root
RUN apk upgrade --no-cache
USER $UID
COPY --chown=$UID:$GID target/*.nar /pulsar/connectors/
