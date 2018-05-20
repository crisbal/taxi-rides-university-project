.DEFAULT_GOAL := all

.PHONY: all mysql mongo dataset

dataset:
	python general/00-dataset.py

mysql:
	python mysql/01-setup.py 
	python mysql/02-import.py

mongo:
	python mongo/01-setup.py
	python mongo/02-import.py 

mysql-perf:
	python mysql/03-queries.py

all: mysql mongo
