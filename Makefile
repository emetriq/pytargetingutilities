TEST_CONTAINER = pytargetingutilities-test-$(shell git rev-parse --short HEAD)
UID=$(shell id -u)
GID=$(shell id -g)

default: clean test
docker_release: build.step docker_release.step
build.step:
	-rm -rf reports
	docker build --build-arg UID=$(UID) --build-arg GID=$(GID) -t $(TEST_CONTAINER) .
	touch build.step

test: test.step

test.step: clean.step build.step
	rm -rf clean.step
	docker run -v $(PWD):/usr/src/app -w /usr/src/app --name $(TEST_CONTAINER) $(TEST_CONTAINER) tox
	touch test.step

clean.step:
	rm -rf build.step
	-docker rm $(TEST_CONTAINER) || true
	-docker rmi $(TEST_CONTAINER) || true
	-rm -rf reports/
	touch clean.step

deploy: test.step
	docker run -v $(PWD):/usr/src/app -w /usr/src/app $(TEST_CONTAINER) /bin/bash -c "tox -e build && tox -e publish"
.PHONY: clean dockerbuild