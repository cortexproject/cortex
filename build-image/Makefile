save-multiarch-build-image:
	@echo
	# Build image for each platform separately... it tends to generate fewer errors.
	$(SUDO) docker buildx build --platform linux/amd64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)build-image:$(IMAGE_TAG)-amd64 --output type=docker,dest=./build-image-amd64.tar build-image/
	$(SUDO) docker buildx build --platform linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)build-image:$(IMAGE_TAG)-arm64 --output type=docker,dest=./build-image-arm64.tar build-image/

load-multiarch-build-image:
	$(SUDO) docker load -i build-image-amd64.tar
	$(SUDO) docker load -i build-image-arm64.tar

push-multiarch-build-image:
	# This command will run the same build as multiarch-build-image, but it will reuse existing platform-specific images,
	# put them together and push to registry.
	$(SUDO) docker push $(IMAGE_PREFIX)build-image:${IMAGE_TAG}-amd64
	$(SUDO) docker push $(IMAGE_PREFIX)build-image:${IMAGE_TAG}-arm64
	$(SUDO) docker manifest create $(IMAGE_PREFIX)build-image:$(IMAGE_TAG) --amend $(IMAGE_PREFIX)build-image:${IMAGE_TAG}-amd64 --amend $(IMAGE_PREFIX)build-image:${IMAGE_TAG}-arm64
	$(SUDO) docker manifest push $(IMAGE_PREFIX)build-image:$(IMAGE_TAG)
