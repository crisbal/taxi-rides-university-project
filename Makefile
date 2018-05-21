.PHONY: import import-mysql import-mongo dataset perf-clean perf-mysql perf-mongo

dataset:
	python general/00-dataset.py

import-mysql:
	python mysql/01-setup.py 
	python mysql/02-import.py

import-mongo:
	python mongo/01-setup.py
	python mongo/02-import.py 

import: import-mysql import-mongo

perf-clean: 
	rm -f benchmarks.json
	echo "{}" > benchmarks.json
	
perf-mysql:
	python mysql/03-queries.py

perf-mongo:
	python mongo/03-queries.py

perf: perf-clean perf-mysql perf-mongo
