# This file is part of Poetry
# https://github.com/sdispater/poetry

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2018 Sébastien Eustace

POETRY_RELEASE := $$(sed -n -E "s/__version__ = '(.+)'/\1/p" esque/__init__.py)

# lists all available targets
list:
	@sh -c "$(MAKE) -p no_targets__ | \
		awk -F':' '/^[a-zA-Z0-9][^\$$#\/\\t=]*:([^=]|$$)/ {\
			split(\$$1,A,/ /);for(i in A)print A[i]\
		}' | grep -v '__\$$' | grep -v 'make\[1\]' | grep -v 'Makefile' | sort"
# required for list
no_targets__:

clean:
	@rm -rf build dist .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

format: clean
	@poetry run black poetry/ tests/

# test your application (tests in the tests/ directory)
test-suite:
	@docker-compose up -d --build

test:
	@poetry run pytest --integration --local tests/

release: build linux_release

build:
	@poetry build

publish:
	@poetry publish

wheel:
	@poetry build -v

linux_release:
	docker pull quay.io/pypa/manylinux2010_x86_64
	docker run --rm -t -i -v `pwd`:/io quay.io/pypa/manylinux2010_x86_64 /io/make-linux-release.sh

# run tests against all supported python versions
tox:
	@tox