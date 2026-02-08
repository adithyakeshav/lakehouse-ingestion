# Lakehouse Ingestion - Build & Deploy
.PHONY: help build upload deploy-dev deploy-prod clean

MINIO_ENDPOINT ?= http://minio.minio.svc.cluster.local:9000
JAR_PATH = target/scala-2.12/lakehouse-ingestion.jar
S3_PATH = s3://spark-jars/lakehouse-ingestion.jar

help:
	@echo "Lakehouse Ingestion - Build & Deploy"
	@echo ""
	@echo "Targets:"
	@echo "  build        - Build JAR with sbt assembly"
	@echo "  upload       - Upload JAR to MinIO"
	@echo "  deploy-dev   - Deploy to dev environment"
	@echo "  deploy-prod  - Deploy to prod environment"
	@echo "  clean        - Clean build artifacts"
	@echo ""
	@echo "Environment variables:"
	@echo "  MINIO_ENDPOINT - MinIO endpoint (default: http://minio.minio.svc.cluster.local:9000)"

build:
	@echo "Building JAR..."
	sbt clean assembly
	@if [ -f $(JAR_PATH) ]; then \
		echo "✅ JAR built: $(JAR_PATH)"; \
	else \
		echo "❌ JAR build failed!"; \
		exit 1; \
	fi

upload: build
	@echo "Uploading JAR to MinIO..."
	@if ! aws --endpoint-url $(MINIO_ENDPOINT) s3 ls s3://spark-jars/ 2>/dev/null; then \
		echo "Creating bucket spark-jars..."; \
		aws --endpoint-url $(MINIO_ENDPOINT) s3 mb s3://spark-jars; \
	fi
	aws --endpoint-url $(MINIO_ENDPOINT) s3 cp $(JAR_PATH) $(S3_PATH)
	@echo "✅ JAR uploaded to $(S3_PATH)"

deploy-dev: upload
	@echo "Deploying to lakehouse-dev..."
	helm upgrade --install lakehouse-ingestion-dev ./helm \
		--namespace lakehouse-dev \
		--create-namespace \
		--values helm/values.yaml \
		--values helm/values-dev.yaml \
		--wait --timeout 10m
	@echo "✅ Deployed to lakehouse-dev"

deploy-prod: upload
	@echo "Deploying to lakehouse-prod..."
	helm upgrade --install lakehouse-ingestion-prod ./helm \
		--namespace lakehouse-prod \
		--create-namespace \
		--values helm/values.yaml \
		--values helm/values-prod.yaml \
		--wait --timeout 10m
	@echo "✅ Deployed to lakehouse-prod"

clean:
	sbt clean
	@echo "✅ Build artifacts cleaned"