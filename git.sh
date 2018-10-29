#!/bin/sh
comment=$1*X
git add --all
git commit -m $comment
git push -u origin master
