#!/bin/sh
if [ $# -ne 1 ]
then
  echo "Must specify IP address"
  exit 1
fi

KEY_FILE=

scp -i $KEY_FILE backend.py duplicator.py frontend.py front_ops.py SendMsg.py ubuntu@$1:/home/ubuntu
