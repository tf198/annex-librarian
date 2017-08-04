
test:
	python -m unittest discover

coverage:
	coverage run --source librarian -m unittest discover
	coverage html
