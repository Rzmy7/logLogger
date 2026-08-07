.PHONY: all build test clean run help

# Variables
APP_NAME := log-analytics-platform
BUILD_DIR := bin

all: build

build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)

test:
	@echo "Running tests..."

clean:
	@echo "Cleaning build directory..."
	@rm -rf $(BUILD_DIR)

help:
	@echo "Available targets:"
	@echo "  build   - Build binary"
	@echo "  test    - Run tests"
	@echo "  clean   - Remove build files"
