.PHONY: import import-mysql import-mongo dataset perf-clean perf-mysql perf-mongo

dataset:
	python general/00-dataset.py

import-mysql:
	python mysql/01-setup.py 
	python mysql/02-import.py

import-mongo:
	python mongo/01-setup.py
	python mongo/02-import.py drop

import: import-mysql import-mongo

perf-clean: 
	rm -f benchmarks.json
	echo "{}" > benchmarks.json
	
perf-mysql:
	python mysql/04-indexes.py drop
	python mysql/03-queries.py noindex

	python mysql/04-indexes.py create 	
	python mysql/03-queries.py indexed

perf-mongo:
	python mongo/04-indexes.py drop
	python mongo/03-queries.py noindex

	python mongo/04-indexes.py create 	
	python mongo/03-queries.py indexed

perf: perf-clean perf-mysql perf-mongo

graphs:
	test -f benchmarks.json
	python general/98-detailed-results.py
	python general/99-graphs.py
