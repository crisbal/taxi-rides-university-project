.PHONY: import import-mysql import-mongo dataset perf-clean perf-mysql perf-mongo

dataset:
	python general/00-dataset.py

import-mysql:
	python mysql/01-setup.py 
	python mysql/02-import.py
	python mysql/02b-import-rides.py 2016_01
	python mysql/02b-import-rides.py 2016_02
	python mysql/02b-import-rides.py 2016_03
	python mysql/02b-import-rides.py 2016_04
	python mysql/02b-import-rides.py 2016_05
	python mysql/02b-import-rides.py 2016_06
	python mysql/02b-import-rides.py 2016_07
	python mysql/02b-import-rides.py 2016_08
	python mysql/02b-import-rides.py 2016_09
	python mysql/02b-import-rides.py 2016_10
	python mysql/02b-import-rides.py 2016_11
	python mysql/02b-import-rides.py 2016_12

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
