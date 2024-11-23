FROM ubuntu:24.04

# -----------------------------------------------------------------------------
# Initialize Image
# -----------------------------------------------------------------------------

# Set environment variables.
ENV \
    LANG=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive \
    DEVTOOLS=1

# Don't ignore errors.
SHELL ["/usr/bin/bash", "-e", "-o", "pipefail", "-c"]

# Install utilty packages that are used by modules in their pre-init phases.
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    gpg \
    lsb-release \
    wget

# Create a directory for storing keyrings.
ARG KEYRING_PATH=/usr/local/share/keyrings
RUN mkdir -p ${KEYRING_PATH}

# -----------------------------------------------------------------------------
# Set Timezone
# -----------------------------------------------------------------------------

# Reset to root user.
USER root

# Setup timezone environment variables.
ENV TZ=America/Toronto

# Install timezone dependencies.
RUN apt-get install -y --no-install-recommends tzdata

# Set the timezone.
RUN <<EOT
    rm -f /etc/timezone /etc/localtime
    ln -sfn /usr/share/zoneinfo/${TZ} /etc/localtime
    echo ${TZ} >/etc/timezone
EOT

# -----------------------------------------------------------------------------
# Create User
# -----------------------------------------------------------------------------

# Reset to root user.
USER root

# Setup user arguments.
ARG USERNAME=developer
ARG USER_UID=1000
ARG USER_GID=1000
ARG USER_SHELL_PACKAGE=bash
ARG USER_SHELL_PATH=/usr/bin/bash

# Install user dependencies.
RUN apt-get install -y --no-install-recommends \
    ${USER_SHELL_PACKAGE} \
    sudo

# Copy the developer mod script.
COPY --chmod=0700 sbin/developermod /usr/local/sbin/developermod

# Modify the default ubuntu user.
RUN <<EOT
    # Ubuntu 24.04 and later define the 'ubuntu' user, which should always exist
    # on the system. It can be modified.
    # https://askubuntu.com/questions/1513927/ubuntu-24-04-docker-images-now-includes-user-ubuntu-with-uid-gid-1000
    # https://bugs.launchpad.net/cloud-images/+bug/2005129
    echo "ubuntu" > /root/developer

    # Modify the default 'ubuntu' user.
    /usr/local/sbin/developermod \
        ${USERNAME} \
        ${USER_UID} \
        ${USER_GID} \
        ${USER_SHELL_PATH}
EOT

# Copy home directory contents.
COPY --chown=${USERNAME}:${USERNAME} home /home/${USERNAME}

# -----------------------------------------------------------------------------
# Install Go
# -----------------------------------------------------------------------------

# Reset to root user.
USER root

# Setup Go environment variables.
ENV GOPATH=/usr/local/share/go
ENV PATH=${GOPATH}/bin:/usr/local/go/bin:$PATH

# Setup Go arguments.
ARG GO_VERSION=1.23.3

RUN <<EOT
    # Create the working directory.
    WORK_DIR="$(mktemp -d)"
    pushd "${WORK_DIR}"

    # Install Go.
    arch="$(dpkg --print-architecture)"; arch="${arch##*-}";
    filename="go${GO_VERSION}.linux-${arch}.tar.gz"
    curl -O -L "https://golang.org/dl/${filename}"
    tar -C /usr/local -xzf "${filename}"

    # Remove the working directory.
    popd
    rm -rf "${WORK_DIR}"

    # Create the GOPATH directory.
    mkdir -p ${GOPATH}
    chown ${USERNAME}:${USERNAME} ${GOPATH}
EOT

# Switch to the non-root user.
USER ${USERNAME}

# Install Go packages.
RUN <<EOT
    go install github.com/go-delve/delve/cmd/dlv@latest
    go install golang.org/x/tools/gopls@latest
    go install honnef.co/go/tools/cmd/staticcheck@latest
EOT

# -----------------------------------------------------------------------------
# Install Docker
# -----------------------------------------------------------------------------

# Reset to root user.
USER root

# Docker install guide:
# https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository

ARG DOCKER_GID=

RUN <<EOT
    # If DOCKER_GID is set, then create the docker group with that GID before
    # the install script creates the group with a different GID.
    if [ -n "${DOCKER_GID}" ]; then
        groupadd -g ${DOCKER_GID} docker
    fi

    # Add Docker keyring.
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o ${KEYRING_PATH}/docker-apt-keyring.gpg

    # Add Docker to source list.
    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=${KEYRING_PATH}/docker-apt-keyring.gpg] \
        https://download.docker.com/linux/ubuntu \
        $(lsb_release -cs) stable" \
        > /etc/apt/sources.list.d/docker.list

    # Update sources.
    apt-get update

    # Install packages.
    apt-get install -y --no-install-recommends \
        containerd.io \
        docker-ce \
        docker-ce-cli \
        docker-buildx-plugin \
        docker-compose-plugin

    # Add user to docker group.
    usermod --append --groups docker ${USERNAME}
EOT

# -----------------------------------------------------------------------------
# Install Postgres Client
# -----------------------------------------------------------------------------

# Reset to root user.
USER root

ARG POSTGRES_CLIENT_MAJOR_VERSION=17

RUN <<EOT
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
    apt-get update
    apt-get install -y --no-install-recommends postgresql-client-${POSTGRES_CLIENT_MAJOR_VERSION}
EOT

# -----------------------------------------------------------------------------
# Install Dbmate
# -----------------------------------------------------------------------------

# Reset to root user.
USER root

ARG AMACNEIL_DBMATE_VERSION=2.23.0

RUN <<EOT
    arch="$(dpkg --print-architecture)"
    arch="${arch##*-}"
    url="https://github.com/amacneil/dbmate/releases/download/v${AMACNEIL_DBMATE_VERSION}/dbmate-linux-${arch}"
    curl -fsSL -o /usr/local/bin/dbmate ${url}
    chmod +x /usr/local/bin/dbmate
EOT

# -----------------------------------------------------------------------------
# Install Utilities
# -----------------------------------------------------------------------------

# Reset to root user.
USER root

# Install utilities.
RUN apt-get install -y --no-install-recommends \
    bind9-host \
    bsdextrautils \
    build-essential \
    bzip2 \
    dirmngr \
    dnsutils \
    file \
    gettext-base \
    git \
    git-lfs \
    gpg-agent \
    htop \
    iproute2 \
    iputils-ping \
    jq \
    less \
    locales \
    lsof \
    net-tools \
    netcat-openbsd \
    openssh-client \
    openssl \
    pkg-config \
    procps \
    psmisc \
    rsync \
    socklog \
    strace \
    tcpdump \
    tree \
    unzip \
    uuid \
    vim \
    xz-utils

# -----------------------------------------------------------------------------
# Finalize Modules
# -----------------------------------------------------------------------------

# Finalize modules as root.
USER root

# Clean up packages.
RUN apt-get clean && rm -rf /var/lib/apt/lists

# -----------------------------------------------------------------------------
# Initialize Container
# -----------------------------------------------------------------------------

# Use the entrypoint script to initialize the container.
COPY --chmod=0755 bin/entrypoint.sh     /usr/local/bin/entrypoint.sh
ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]

# Switch to the developer user.
USER ${USERNAME}

# Sleep while attached.
CMD /bin/sh -c "while sleep 60; do :; done"
