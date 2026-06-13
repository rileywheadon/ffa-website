set -e -o pipefail

jj bookmark set master > /dev/null 2>&1
jj git push --remote=origin > /dev/null 2>&1

echo "Building and pushing Docker image..."
docker build -f Dockerfile.flask -t rileywheadon/ffa-website-flask:latest .
docker build -f Dockerfile.plumber -t rileywheadon/ffa-website-plumber:latest .
docker push rileywheadon/ffa-website-flask:latest
docker push rileywheadon/ffa-website-plumber:latest
echo ""

# TODO: Replace this with something more robust
echo "Redeploying the application..."
cd ../personal-platform
./deploy.sh
