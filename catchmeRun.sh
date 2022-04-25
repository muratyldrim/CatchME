#!/usr/bin/sh

# predictModels.py runs in a loop in the backgroud.

while :
do /ml/projects/catchme/predictModels.py ; sleep 30
done
