#!/bin/bash

if [ "$EUID" -ne 0 ]
  then echo "Please run as root (e.g. sudo $0)"
  exit 1
fi

set -e
set -x

echo "Work in progress..."
