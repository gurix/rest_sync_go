docker pull mongo
#!/bin/bash

# Check if a container named "mongodb" exists
if docker ps -a | grep -q mongodb; then
    echo "Starting existing mongodb container..."
    docker start mongodb
else
    echo "Running a new mongodb container..."
    docker run --name mongodb -p 27017:27017 mongo
fi
