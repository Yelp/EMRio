PYTHON=PYTHONPATH="$(shell pwd)" unit2 discover

test: 
	$(PYTHON) -v -s tests -t .
