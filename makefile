# Airflow Docker Compose Makefile
# Usage: make [command]

COMPOSE_FILE = docker-compose.yml
PROJECT_NAME = airflow-local

# Default target
.DEFAULT_GOAL := help

.PHONY: help up start stop down restart init reset clean logs

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

init: ## Initialize Airflow (first time setup)
	@echo "Initializing Airflow..."
	@mkdir -p ./dags ./logs ./plugins ./config
	@docker compose -f $(COMPOSE_FILE) up airflow-init

up: ## Start all services
	@echo "Starting Airflow services..."
	@docker compose -f $(COMPOSE_FILE) up -d

start: up ## Alias for up

stop: ## Stop all services (keeps containers)
	@echo "Stopping Airflow services..."
	@docker compose -f $(COMPOSE_FILE) stop

down: ## Stop and remove all containers
	@echo "Stopping and removing containers..."
	@docker compose -f $(COMPOSE_FILE) down

restart: down up ## Restart all services

view: ## View logs from all containers (use Ctrl+C to exit)
	@echo "Showing logs from all Airflow containers..."
	@docker compose -f $(COMPOSE_FILE) logs -f

view-service: ## View logs from specific service (usage: make logs-service SERVICE=webserver)
	@echo "Showing logs from $(SERVICE) service..."
	@docker compose -f $(COMPOSE_FILE) logs -f $(SERVICE)

reset: ## Reset everything - remove containers, volumes, and reinitialize
	@echo "Resetting Airflow environment..."
	@echo "This will remove all containers, volumes, and data. Continue? [y/N]" && read ans && [ $${ans:-N} = y ]
	@docker compose -f $(COMPOSE_FILE) down -v
	@docker system prune -f --volumes
	@echo "Cleaning local directories..."
	@sudo rm -rf ./logs/* ./plugins/* 2>/dev/null || rm -rf ./logs/* ./plugins/* 2>/dev/null || true
	@echo "Reinitializing..."
	@$(MAKE) init
	@$(MAKE) up

clean: ## Remove containers and volumes (without confirmation)
	@echo "Cleaning up containers and volumes..."
	@docker compose -f $(COMPOSE_FILE) down -v
	@docker system prune -f --volumes
