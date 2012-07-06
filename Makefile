UNIT2=PYTHONPATH="$(shell pwd)" unit2

test: 
	$(UNIT2) discover -v -s tests -t .
