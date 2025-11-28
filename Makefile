docker-build:
	docker build -t ingestion-job:latest .

deploy:
	kubectl apply -f job.yaml