#!/bin/sh
comment=$1
today=`zdump EST`
if [ -z "$1" ]
  then
    echo "Please provide user comments:"
    read comment
    if [ -z "$comment" ]
      then
        comment="Current commit is on "$today
    fi
fi
echo "User comments: "  $comment
echo ""
git add --all
git commit -m "$comment"
git push -u origin master
