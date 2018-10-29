#!/bin/sh
comment=$1
if [ -z "$1" ]
  then
    echo "Please provide user comments"
    read comment
fi
echo "User comments: "  $comment
echo ""
git add --all
git commit -m "$comment"
git push -u origin master
