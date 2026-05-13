#!/bin/sh
echo "$1" | base64 -d > ../conf/$2
