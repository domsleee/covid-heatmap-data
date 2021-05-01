#!/bin/bash
set -ex
tmp=/tmp/covid-heatmap
! rm -rf $tmp
! mkdir -p "$tmp"

cd $tmp
git clone https://github.com/domsleee/covid-heatmap.git
cd covid-heatmap/dissolve-polygons
npm i
node -r esm main.js --outputFile $tmp/suburb-10-nsw-proc.geojson
cd $tmp
ssh-agent bash -c 'ssh-add /home/dom/.ssh/id_rsa_3; git clone git@github.com:domsleee/covid-heatmap-data.git'
cd covid-heatmap-data
cp $tmp/suburb-10-nsw-proc.geojson ./docs/
git config --local user.name "domslee-bot"
git config --local user.email "domsleebot@gmail.com"
git add .
! git commit -m "Update data"
ssh-agent bash -c 'ssh-add /home/dom/.ssh/id_rsa_3; git push'
