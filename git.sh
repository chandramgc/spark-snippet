#!/bin/sh

comment=$1

git add .

git commit -m $comment

echo "commit finished,push to origin master  ? "

read commit

case $commit in 
y|Y|YES|yes)
git push -u origin master
;;
n|NO|N|no)
 exit 0

esac
