# Needs to be defined before including Makefile.common to auto-generate targets
DOCKER_ARCHS ?= amd64 ppc64le
DOCKER_REPO	 ?= treydock
export GOPATH ?= $(firstword $(subst :, ,$(shell go env GOPATH)))
GOLANGCI_LINT_VERSION ?= v2.12.2

include Makefile.common

DOCKER_IMAGE_NAME ?= tsm_exporter

coverage:
	go test -race -coverpkg=./... -coverprofile=coverage.txt -covermode=atomic ./...
