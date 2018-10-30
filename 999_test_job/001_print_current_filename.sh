#!/bin/bash
fileName=`basename "$0"`
name="$(echo $fileName | cut -d'.' -f1)"
echo $name
