#!/bin/sh
comment=$1
today=`zdump EDT`
if [ -z "$1" ]
  then
    echo "Please provide user comments:"
    read comment
    if [ -z "$comment" ]
      then
        comment=$today
    fi
fi
echo "User comments: "  $comment
echo ""
git add --all
git commit -m "$comment"
git push -u origin master
