# Annex Librarian #

Xapian based search engine for your annexed data.  Incrementally updates based on
the annex data so everything important stays with the repo - can remove the _.git/librarian_
folder and everything will still come back to the same state.

You can build the index on __any__ clone without pulling in the annexed files.  The web interface will
pull them in from another location on demand.

Includes basic inspectors for `images` and `PDFs` that create `json` representations of the objects but
these are optional and you can implement your own indexing strategy if required.

Adds a couple of extra files to the `git-annex` branch:

111/222/SHA256E-s12--aa...ff.info: JSON representation of the file.

111/222/SHA256E-s12-aa...ff.jpg: Preview image - should be less than 10K.  This can be annexed - TODO: figure 
out how we exclude these....

## Status ##
**Work in progress** The xapian schema may change, but a `git librarian sync -f` will repair that
(even though it may take a few minutes).

The indexers try and limit themselves to a few fields to minimise the chance of overriding user metadata.
See below for fields to avoid if you are going to use the indexers.  If the indexers change and are re-run 
then the fields they generate will be overwritten but others left as is.

## Installation ##
Currently tested on python2.7 and python3.5.

Requires _python_, _python-xapian_

	pip install -r requirements.txt

and then ensure _git-librarian_ is on your path.  If you use virtualenv you will need the `--system-site-packages`
option so it can find you xapian library.

## Usage ##

From the command line:

	# index new commits
	git librarian sync

	# use xapian query syntax
	git librarian search -- tag:special +date:201703* -tag:boring

Web interface:

	git librarian server
	# then visit http://localhost:7920

## Indexers ##

### Unindexed ###
Some pseudo properties are stored in the search engine

* added:<first commit date> (DA:200170101)
* state:untagged (XS:untagged)
* inspector:none (XI:none)

### File ###
Uses `stat` for some basic properties

* date - uses the ctime property
* ext - result of `os.path.splitext`
* mimetype - split content-type e.g. `['text', 'plain']`
* size - in kB with suffix e.g `23kB`

### Image ###

* date - from EXIF DateTimeOriginal
* props - Various image properties
* device - EXIF make and model

Properties are all stored under the `props` key and are

* orientation - `landscape` or `portrait`
* aspect - e.g. `4:3`
* pano - if landscape and aspect > 2
* resolution - e.g. `300dpi`
* colour: `RGB` or `BW`

### Data structure

	{
		"_docid": 1234,
		"librarian": {
			"inspector": ["file-1.0.0", "image-1.0.1"]
		},
		"annex": {
			"added": <date>,
			...	
		},
		"git": {
			branches: {
				"master": "boats/canoe.jpg"
			},
		},
		"meta": {
			"tag": ["boat", "blue"],
			...
		},
		"image": {
			"props": [],
			"date": <date>,
			"device": ["make", "model"]
			...
		},
		"file": {
			"ctime": <date>,
			"ext": "jpg",
			...	
		}
	}

## TODO ##

### Librarian ###

### GUI/API ###

* make GUI a little more robust.
* add whitelist for /api/cli.
* make `server` serve multiple librarian instances.
* implement secure public sharing based on tag/gallery/search.
