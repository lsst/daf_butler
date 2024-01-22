# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# This Dockerfile is based on the fastapi_safir_app template from
# lsst/templates
#
# This Dockerfile has four stages:
#
# base-image
#   Updates the base Python image with security patches and common system
#   packages. This image becomes the base of all other images.
# dependencies-image
#   Installs third-party dependencies (requirements/main.txt) into a virtual
#   environment. This virtual environment is ideal for copying across build
#   stages.
# install-image
#   Installs the app into the virtual environment.
# runtime-image
#   - Copies the virtual environment into place.
#   - Runs a non-root user.
#   - Sets up the entrypoint and port.

FROM python:3.11.6-slim-bullseye as base-image

# Update system packages
COPY server/scripts/install-base-packages.sh .
RUN ./install-base-packages.sh && rm ./install-base-packages.sh

FROM base-image AS dependencies-image

# Install system packages only needed for building dependencies.
COPY server/scripts/install-dependency-packages.sh .
RUN ./install-dependency-packages.sh

# Create a Python virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
# Make sure we use the virtualenv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
# Put the latest pip and setuptools in the virtualenv
RUN pip install --upgrade --no-cache-dir pip setuptools wheel

# Install the app's Python runtime dependencies
COPY requirements/docker.txt ./docker-requirements.txt
RUN pip install --no-cache-dir -r docker-requirements.txt

# Install dependencies only required by unit tests in a separate image for better caching
FROM dependencies-image AS test-dependencies-image
RUN apt-get update
RUN apt-get install -y --no-install-recommends postgresql postgresql-pgsphere
COPY requirements/docker-test.txt ./docker-test-requirements.txt
RUN pip install --no-cache-dir -r docker-test-requirements.txt

# Run unit tests
FROM test-dependencies-image AS unit-test
RUN useradd --create-home testuser
COPY . /workdir
WORKDIR /workdir
RUN pip install --no-cache-dir --no-deps .
# The postgres tests refuse to run as root
USER testuser
# For some reason Butler unit tests create temporary files in the source code tree
# unless you explicitly specify otherwise.
RUN TMPDIR=/tmp DAF_BUTLER_TEST_TMP=/tmp pytest -x

FROM dependencies-image AS install-image

# Use the virtualenv
ENV PATH="/opt/venv/bin:$PATH"

COPY . /workdir
WORKDIR /workdir
RUN pip install --no-cache-dir --no-deps .

FROM base-image AS runtime-image

# Create a non-root user
RUN useradd --create-home appuser

# Copy the virtualenv
COPY --from=install-image /opt/venv /opt/venv

# Make sure we use the virtualenv
ENV PATH="/opt/venv/bin:$PATH"

# Switch to the non-root user.
USER appuser

# Expose the port.
EXPOSE 8080

# Run the application.
CMD ["uvicorn", "lsst.daf.butler.remote_butler.server:create_app", "--host", "0.0.0.0", "--port", "8080"]
