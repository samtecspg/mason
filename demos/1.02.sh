
#!/bin/bash
set -e

echo "Requirements:  docker"
echo "Press [Enter] to continue demo:"
read -p "$*"

cd ..
./docker_build

docker run -it --env-file=.env -v=$HOME/.kube/:/app/.kube/ samtecspg/mason:v1.02 ./demos/run_demo.sh 1.02
