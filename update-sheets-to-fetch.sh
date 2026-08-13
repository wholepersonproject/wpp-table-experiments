#!/bin/bash

rm sheets_to_fetch.csv
for f in ../wpp-kg/digital-objects/wpp/*/draft/raw/*.csv; do
  echo $(echo $f | cut -d '/' -f 5),$(echo $f | cut -d '/' -f 8),$(cat $f.url) >> sheets_to_fetch.csv
done
