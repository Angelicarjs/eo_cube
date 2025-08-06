#!/bin/bash

# Earth Observation Cube - Docker Startup Script

echo "🚀 Starting Earth Observation Cube Environment..."
echo "================================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Build and start the services
echo "📦 Building and starting services..."
docker-compose up --build -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Show service status
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "🌐 Access URLs:"
echo "   Jupyter Lab: http://localhost:8888"
echo "   Dask Scheduler: http://localhost:8786"
echo "   Dask Dashboard: http://localhost:8787"
echo ""
echo "🔑 Jupyter Token: your_token_here"
echo ""
echo "📝 To stop the services, run: docker-compose down"
echo "📝 To view logs, run: docker-compose logs -f" 