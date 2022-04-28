#!/bin/sh

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_DEFAULT_REGION=ca-central-1

cd /
cd home/rpi-sunroom/rpi-temperature-collector
/usr/bin/python3 main.py sunroom -a
cd /