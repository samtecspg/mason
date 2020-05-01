
#!/bin/bash
set -e

echo "Requirements:  docker, .env file with AWS credentials.  Run docker_build in parent directory.  Run script from demos root."
echo "Press [Enter] to continue demo:"
read -p "$*"

cd ..

docker run -it --env-file=.env -v=$HOME/.kube/:/app/.kube/ samtecspg/mason:v1.03 ./demos/run_demo.sh 1.03a
