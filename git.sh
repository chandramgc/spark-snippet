#!/bin/sh
comment=$1
git add --all
git commit -m "$comment"
git push -u origin master
