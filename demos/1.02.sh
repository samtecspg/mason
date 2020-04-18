
#!/bin/bash
set -e

echo "Requirements:  docker"
echo "Press [Enter] to continue demo:"
read -p "$*"

docker run -it samtecspg/mason:v1.02 /bin/sh demos/run_demo.sh 1.02

