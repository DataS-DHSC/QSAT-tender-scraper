# `docs` folder overview

All documentation for the project should be included in this folder in either
reStructuredText or Markdown files, with acceptable formatting for Sphinx. [Guidance on
how to write Sphinx documentation is supplied in the contributor
guide](docs/user_guide/writing_sphinx_documentation.md)

To build the documentation:

* In a Windows console (making sure you are running the console from the `qsat-tender-scraper-env` virtual envrionment), run the following from the *project root directory*. 

```shell
sphinx-apidoc -o docs ./src
```

This will generate necessary files to create the docs.

* To generate the html docs themselves you can run:

```shell
cd docs
./make.bat html
```


The HTML-version of this documentation can then be viewed at `docs/_build/index.html`,
relative to the top-level of this repository.