.PHONY: all build test clean swagger run help

# Variables
APP_NAME := log-analytics-platform
BUILD_DIR := bin

all: build

build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)

swagger:
	@echo "Generating Swagger documentation..."
	@go run github.com/swaggo/swag/cmd/swag@latest init -g cmd/ingestor/main.go -o docs/swagger

test:
	@echo "Running tests..."

clean:
	@echo "Cleaning build directory..."
	@rm -rf $(BUILD_DIR)

help:
	@echo "Available targets:"
	@echo "  build   - Build binary"
	@echo "  swagger - Generate Swagger API docs"
	@echo "  test    - Run tests"
	@echo "  clean   - Remove build files"
