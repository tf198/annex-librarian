Annex Librarian
===============

Xapian based indexer for your annex

Installation
------------
Currently uses python2.7 - will add six soon...

Requires _python_, _python-xapian_

	pip install -r requirements.txt

and then ensure _git-librarian_ is on your path.

Usage
-----

From the command line:

	# index new commits
	git librarian sync

	# use xapian query syntax
	git librarian search -- tag:special +date:2017-03* -tag:boring

Web interface:

	git librarian server
	# then visit http://localhost:7920
