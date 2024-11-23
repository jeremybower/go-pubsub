#!/bin/bash

# Exit on error.
set -euo pipefail

# Update our trusted root certificates.
sudo update-ca-certificates

# Reset GPG and safe directories to known values.
git config --global --replace-all gpg.program /usr/bin/gpg
git config --global --replace-all safe.directory /workspace

# Run the specified commands.
exec "$@"
