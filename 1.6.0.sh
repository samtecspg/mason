
#!/bin/bash
set -e

echo "Requirements:  docker, .env file with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, GLUE_ROLE_ARN with full glue permission.  Run docker_build in parent directory.  Run script from demos root."
echo "Press [Enter] to continue demo:"
read -p "$*"

cd ..

docker run -it --env-file=.env -v=$HOME/.kube/:/app/.kube/ samtecspg/mason:v1.6.0 ./demos/run_demo.sh 1.6.0
