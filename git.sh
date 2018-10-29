#!/bin/sh
comment=$1
echo "User comments: " +  $comment
git add --all
git commit -m "$comment"
git push -u origin master
