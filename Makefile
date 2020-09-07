# $(MAKE) all of the things

# Automation Installs ----------------------------------------------------------
install-python-tools:
	pip install --upgrade pip && pip install pipenv

install-serverless:
	npm install -g serverless@1.51.0

# Supportal --------------------------------------------------------------------
test-supportal:
	cd ./supportal && $(MAKE) test && cd -

test-supportal-on-ubuntu:
	cd ./supportal && $(MAKE) -j $(nproc) install-dev-ubuntu && cd -
	cd ./supportal && $(MAKE) test && cd -

deploy-supportal:
	cd ./supportal && $(MAKE) -j $(nproc) deploy-with-preflight && cd -

# Pollaris ---------------------------------------------------------------------
test-pollaris:
	cd ./pollaris && $(MAKE) test && cd -

deploy-pollaris:
	cd ./pollaris && $(MAKE) -j $(nproc) deploy-with-preflight && cd -

build-pollaris:
	./container-utils build-image pollaris

publish-pollaris: build-pollaris
	./container-utils publish-image pollaris

# EW Common --------------------------------------------------------------------
test-ew-common:
	cd ./ew_common && $(MAKE) test && cd -

# Toes -------------------------------------------------------------------------
test-toes:
	cd ./toes && $(MAKE) test && cd -

# Mission Control --------------------------------------------------------------
deploy-mission-control:
	(export STAGE=prod && cd ./mission_control && $(MAKE) -j $(nproc) deploy && cd -)

# Sheet Sync -------------------------------------------------------------------
deploy-sheet-sync:
	(export STAGE=prod && cd ./sheet_sync && $(MAKE) -j $(nproc) deploy && cd -)

# Redhook ----------------------------------------------------------------------
test-redhook:
	cd ./redhook && $(MAKE) test && cd -

deploy-redhook:
	cd ./redhook && $(MAKE) -j $(nproc) deploy && cd -

# Blitz ------------------------------------------------------------------------
test-blitz:
	cd ./blitz && $(MAKE) test && cd -

deploy-blitz:
	cd ./blitz && $(MAKE) -j $(nproc) deploy && cd -

# Civistrack -------------------------------------------------------------------
test-civistrack:
	cd ./civistrack && $(MAKE) test && cd -

deploy-civistrack:
	cd ./civistrack && $(MAKE) -j $(nproc) deploy && cd -

# Civility ---------------------------------------------------------------------
build-civility:
	./container-utils build-image civility

publish-civility: build-civility
	./container-utils publish-image civility

# ThePike ----------------------------------------------------------------------
test-thepike:
	cd ./thepike && $(MAKE) test && cd -

deploy-thepike:
	cd ./thepike && $(MAKE) -j $(nproc) deploy && cd -

# Airflow ----------------------------------------------------------------------
build-airflow-image:
	./container-utils build-image airflow ./airflow/images/airflow

publish-airflow-image: build-airflow-image
	./container-utils publish-image airflow false

build-airflow-task-image:
	./container-utils build-image airflow-task ./airflow/images/task

publish-airflow-task-image: build-airflow-task-image
	./container-utils publish-image airflow-task

build-datascience-python-image:
	./container-utils build-image datascience-python ./airflow/images/datascience-python

publish-datascience-python-image: build-datascience-python-image
	./container-utils publish-image datascience-python

# Test All ---------------------------------------------------------------------
.PHONY: test-all
test-all: test-supportal test-ew-common test-toes test-redhook test-pollaris test-blitz
