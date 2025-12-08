ARCH := $(shell uname -m)
PLATFORM := manylinux2014_${ARCH}
PACKAGE_VERSION ?= 0.0.0

.PHONY: build install clean

build:
	python3 setup.py bdist_wheel --plat-name ${PLATFORM}

install:
	pip3 uninstall -y runai_model_streamer_azure && \
	pip3 install dist/runai_model_streamer_azure-${PACKAGE_VERSION}-py3-none-${PLATFORM}.whl

clean:
	rm -rf build/ dist/ runai_model_streamer_azure.egg-info/
