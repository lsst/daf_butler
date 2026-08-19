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

FROM python:3.14.7-slim-trixie AS base-image

# Update system packages
COPY server/scripts/install-base-packages.sh .
RUN ./install-base-packages.sh && rm ./install-base-packages.sh

FROM base-image AS dependencies-image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1
ENV UV_PYTHON_DOWNLOADS=0

# Install system packages only needed for building dependencies.
COPY server/scripts/install-dependency-packages.sh .
RUN ./install-dependency-packages.sh

# Create a virtual environment and install Python libraries.  These are
# installed separately from the daf_butler source code to improve caching.
ARG NEEDED_EXTRAS="--extra postgres --extra server"
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project ${NEEDED_EXTRAS}

# Install daf_butler source code into the venv.
FROM dependencies-image AS install-image
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable ${NEEDED_EXTRAS}

# Run unit tests
FROM dependencies-image AS unit-test
RUN apt-get update
RUN apt-get install -y --no-install-recommends postgresql postgresql-pgsphere
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --dev ${NEEDED_EXTRAS}

COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --dev ${NEEDED_EXTRAS}

# The postgres tests refuse to run as root
RUN useradd --create-home testuser
USER testuser
# For some reason Butler unit tests create temporary files in the source code tree
# unless you explicitly specify otherwise.
RUN TMPDIR=/tmp DAF_BUTLER_TEST_TMP=/tmp uv run pytest -x

FROM base-image AS runtime-image

# Create a non-root user
RUN useradd --create-home appuser

# Copy the virtualenv
COPY --from=install-image /app/.venv /app/.venv

# Make sure we use the virtualenv
ENV PATH="/app/.venv/bin:$PATH"

# Switch to the non-root user.
USER appuser

# Expose the port.
EXPOSE 8080

# Run the application.
CMD ["uvicorn", "lsst.daf.butler.remote_butler.server:create_app", "--host", "0.0.0.0", "--port", "8080"]
