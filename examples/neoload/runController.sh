#!/usr/bin/env bash
set -e
nlVersion=${1:-nlVersionNotSet}
nlWebToken=${2:-tokenNotSet}
nlWebZone=${3:-defaultzone}
containerName=${4:-controller_"$nlWebZone"_$nlVersion}

sudo docker run -d --name $containerName -e MODE=Managed \
  -e NEOLOADWEB_TOKEN=$nlWebToken \
  -e ZONE=$nlWebZone \
  --restart=always  \
  neotys/neoload-controller:$nlVersion
