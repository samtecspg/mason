
#!/bin/bash
set -e

echo "Requirements:  docker.  Run docker_build in parent directory."
echo "Press [Enter] to continue demo:"
read -p "$*"

cd ..

docker run -it --env-file=.env -v=$HOME/.kube/:/app/.kube/ samtecspg/mason:v1.02 ./demos/run_demo.sh 1.02
