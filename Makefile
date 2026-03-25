# Repository Configuration
REPOSITORY_NAME ?= $(shell basename $(CURDIR))

# AWS Configuration
configure-aws-profile:
	aws configure set region $(AWS_REGION) --profile $(AWS_PROFILE)
	@if [ -n "$(TF_VAR_assume_role_arn)" ]; then \
		aws configure set role_arn $(TF_VAR_assume_role_arn) --profile $(AWS_PROFILE); \
		aws configure set source_profile $(AWS_PROFILE) --profile $(AWS_PROFILE); \
	fi
	aws configure set duration_seconds 3600
	aws configure set aws_access_key_id $(AWS_ACCESS_KEY_ID) --profile $(AWS_PROFILE)
	aws configure set aws_secret_access_key $(AWS_SECRET_ACCESS_KEY) --profile $(AWS_PROFILE)

# Docker Image Tag Generation
get-docker-image-tag:
	@if [[ "$(GITHUB_EVENT_NAME)" == "push" && "$(GITHUB_REF)" == "refs/heads/main" ]]; then \
		IMAGE_VERSION="branch-$(BRANCH_NAME)"; \
	else \
		IMAGE_VERSION="pr-$(GITHUB_EVENT_NUMBER)-$(BRANCH_NAME)"; \
	fi; \
	IMAGE_VERSION=$$(echo "$$IMAGE_VERSION" | sed 's|/|-|g;s|branch-main|main|'); \
	echo "IMAGE_VERSION=$$IMAGE_VERSION" >> $(GITHUB_ENV)

# ECR Repository Setup
setup-ecr-repository:
	@REPO_EXISTS=$$(aws ecr describe-repositories --repository-names $(REPOSITORY_NAME) --query 'repositories[0].repositoryName' --output text --region $(AWS_REGION) --profile $(AWS_PROFILE) 2>/dev/null || echo ""); \
	if [ -z "$$REPO_EXISTS" ]; then \
		aws ecr create-repository --repository-name $(REPOSITORY_NAME) --region $(AWS_REGION) --profile $(AWS_PROFILE); \
	fi; \
	REPO_URL=$$(aws ecr describe-repositories --repository-names $(REPOSITORY_NAME) --query 'repositories[0].repositoryUri' --output text --region $(AWS_REGION) --profile $(AWS_PROFILE)); \
	echo "ECR_REPOSITORY_URL=$$REPO_URL" >> $(GITHUB_ENV)

# Docker Login to ECR
docker-login:
	aws ecr get-login-password --profile $(AWS_PROFILE) --region $(AWS_REGION) | \
	docker login --password-stdin --username AWS $(ECR_REPOSITORY_URL)

# Handle ECR Repository with Terraform
handle-ecr-repository:
	cd terraform && \
	terraform import aws_ecr_repository.repository $(REPOSITORY_NAME) || echo "Repository doesn't exist in AWS yet" && \
	terraform apply -target=aws_ecr_repository.repository -auto-approve && \
	echo "ECR_REPOSITORY_URL=$$(terraform output --raw ecr_repository_url)" >> $(GITHUB_ENV)

# Docker Image Cleanup
docker-purge:
	@IMAGE_IDS=$$(aws ecr describe-images \
	--repository-name $(REPOSITORY_NAME) \
	--profile "$(AWS_PROFILE)" \
	--region "$(AWS_REGION)" | jq -r '[.imageDetails[] | select((.imageTags // []) | all(. != "main") or length == 0) | .imageDigest]'); \
	if [ -z "$$IMAGE_IDS" ] || [ "$$IMAGE_IDS" == "[]" ]; then \
		echo "No images found to delete."; \
		exit 0; \
	fi; \
	for IMAGE_ID in $$(echo "$$IMAGE_IDS" | jq -r '.[]'); do \
		echo "Deleting image with ID: $$IMAGE_ID"; \
		aws ecr batch-delete-image \
			--repository-name $(REPOSITORY_NAME) \
			--image-ids imageDigest="$$IMAGE_ID" \
			--profile "$(AWS_PROFILE)" \
			--region "$(AWS_REGION)"; \
	done


# Help
help:
	@echo "Available commands:"
	@echo "  configure-aws-profile    - Configure AWS CLI profile"
	@echo "  get-docker-image-tag     - Generate Docker image tag based on branch/PR"
	@echo "  setup-ecr-repository     - Setup ECR repository if it doesn't exist"
	@echo "  docker-login             - Login to ECR"
	@echo "  handle-ecr-repository    - Handle ECR repository with Terraform"
	@echo "  docker-purge             - Clean up old Docker images from ECR"
