.PHONY: build install clean

build:
	python setup.py sdist bdist_wheel

install:
	pip install -e .

clean:
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
