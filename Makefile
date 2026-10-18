.DEFAULT_GOAL := help
SHELL := /bin/bash

ifneq (,$(wildcard .env))
include .env
export
endif

GO ?= go
IMAGE ?= market-data-service:local

.PHONY: help env run test lint fmt build check openapi docs docker clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

env: ## Create .env from .env.example when missing
	@test -f .env || cp .env.example .env

run: env ## Run the service locally
	$(GO) run ./cmd

test: ## Run unit tests
	$(GO) test ./...

lint: ## Vet packages
	$(GO) vet ./...

fmt: ## Format Go sources
	gofmt -w .

build: ## Compile all packages
	$(GO) build ./...

check: lint test openapi ## Lint, test and validate the API contract

openapi: ## Lint the OpenAPI document
	npx --yes @redocly/cli@1 lint openapi.yaml

docs: ## Serve the OpenAPI document on http://localhost:8089
	docker run --rm -p 8089:8080 -e SWAGGER_JSON=/spec/openapi.yaml -v "$(PWD):/spec" swaggerapi/swagger-ui

docker: ## Build the container image
	docker build -t $(IMAGE) .

clean: ## Remove build artifacts
	$(GO) clean
