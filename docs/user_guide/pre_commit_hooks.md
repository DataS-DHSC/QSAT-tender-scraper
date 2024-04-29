# Pre-commit hooks

[This repository uses the Python package `pre-commit` to manage pre-commit
hooks][pre-commit]. Pre-commit hooks are actions which are run automatically, typically
on each commit, to perform some common set of tasks. For example, a pre-commit hook
might be used to run any code linting automatically before code is committed, ensuring
common code quality.

## Purpose

For this repository, we are using `pre-commit` for a number of purposes:

- checking for secrets being committed accidentally — there is a strict [definition of
  a "secret"](#definition-of-a-secret-according-to-detect-secrets)
- cleaning Jupyter notebooks, which means removing all outputs, execution counts,
  Python kernels, and, for Google Colaboratory (Colab), stripping out user information.

We have configured `pre-commit` to run automatically on every commit. By running on
each commit, we ensure that `pre-commit` will be able to detect all contraventions and
keep our repository in a healthy state.

[*Note*] if you try to make a commit in an environment that does not have access to the pre-commit hook packages the hooks will fail.
 Activate your environment with `conda`: `conda activate qsat-tender-scraper-env` and commit your changes again.


```{note} Pre-commit hooks and Google Colab
No pre-commit hooks will be run on Google Colab notebooks pushed directly to GitHub.
For security reasons, it is recommended that you manually download your notebook, and
commit up locally to ensure pre-commit hooks are run on your changes.
```


The pre-commit hooks installed with this project include:

* [*nbstripout*][nbstripout] - Clears outputs of Jupyter notebooks - this hook will change your code
* [*isort*][isort] - sorts python imports - this hook will change your code
* [*black*][black] - formats code to be inline with the [PEP8 style guide for pyhton code](https://peps.python.org/pep-0008/) - this hook will change your code
* [*flake8*][flake8] - 'lints' code, checking for formatting and syntax errors. this hook will *not* change your code for you, but will provide instructions on how to change it
* [*nbqa*][nbqa] - applys black and isort to jupyter notebooks. 
* [*detect-secrets*][detect-secrets] - *attempts* to identify secret within code. This should be considered as a complement to manually checking for secrets, not a replacemet. This hook will not change your code - but will alert you to the presence of  possible secrets.
* [*bandit*][bandit] -  *attempts* to identify security risks within code. This hook will not change your code - but will alert you to the presence of possible security risks.

## Installation

If you have run `run_setup.bat` - then pre-commit will be installed into your virtual environment and configured for you.

If you are setting up your own environment, you will need to install and configure it yourself

- [install][pre-commit-install] the `pre-commit` package into your environment

- Install each of packages listed above under 'The pre-commit hooks installed with this project include'

- run `pre-commit install` in your terminal to set up `pre-commit` to run when code is
  committed.
  
## Using the `Flake8` pre-commit hook

Flake8 is a wrapper around a a number of linters. 
Flake8 will display warnings for issues in your code in the following format 

`file:{line_number}:{character_number}: {error_code} {error detail}`

for example 

`myfile.py:28:80: E501 line too long (98 > 79 characters)`

Is telling us that in the file `myfile.py` on line 28 there is a "line too long error" (code E501)

#### Inline ingoring flake8 errors

Flake8 will prevent you from commiting until you have resolved the issues in your code.

However sometimes we might have a geniune reason for an issue. 

In these situation you can ignore specific errors on a line with `# noqa: <error>`

For example:
`# noqa: E501` at the end of line 28 would ignore the line too long error

## Using the `detect-secrets` pre-commit hook

```{note} Secret detection limitations
The `detect-secrets` package does its best to prevent accidental committing of secrets,
but it may miss things. Instead, focus on good software development practices! See the
[definition of a secret for further
information](#definition-of-a-secret-according-to-detect-secrets).
```

### Definition of a "secret" according to `detect-secrets`

The `detect-secrets` documentation, as of January 2021, says it works:

> ...by running periodic diff outputs against heuristically crafted \[regular
> expression\] statements, to identify whether any new secret has been committed.

This means it uses regular expression patterns to scan your code changes for anything
that looks like a secret according to the patterns. By definition, there are only a
limited number of patterns, so the `detect-secrets` package cannot detect every
conceivable type of secret.

To understand what types of secrets will be detected, read the `detect-secrets`
documentation on caveats, and the list of supported plugins. Also, you should use
secret variable names with words that will trip the KeywordDetector plugin; see the
[`DENYLIST` variable for the full list of words][detect-secrets-keyword-detector].

### If `pre-commit` detects secrets during commit

If `pre-commit` detects any secrets when you try to create a commit, it will detail
what it found and where to go to check the secret.

If the detected secret is a false positive inline allowlisting can be used to resolve this
and prevent your commit from being blocked.

In either case, if an actual secret is detected (or a combination of actual secrets and
false positives), first remove the actual secret. Then following either of these
processes.  

#### Inline allowlisting

To exclude a false positive, add a `pragma` comment such as:

```python
secret = "Password123"  # pragma: allowlist secret
```

or

```python
#  pragma: allowlist nextline secret
secret = "Password123"
```

If the detected secret is actually a secret (or other sensitive information), remove
the secret and re-commit; there is no need to add any `pragma` comments.

If your commit contains a mixture of false positives and actual secrets, remove the
actual secrets first before adding `pragma` comments to the false positives.

## Using Bandit

Bandit is a security linter for Python.
 It helps identify security vulnerabilities and issues in your Python code before committing changes.

### Using #nosec
In some cases, you may want to suppress Bandit warnings for specific lines of code. 
You can do this by adding #nosec at the end of the line where the issue occurs. For example:

python
```
# Example of insecure code
os.system("rm -rf /")  #nosec
```
## Keeping specific Jupyter notebook outputs

It may be necessary or useful to keep certain output cells of a Jupyter notebook, for
example charts or graphs visualising some set of data. To do this, [according to the
documentation for the `nbstripout` package][nbstripout], either:

1. add a `keep_output` tag to the desired cell; or
2. add `"keep_output": true` to the desired cell's metadata.

You can access cell tags or metadata in Jupyter by enabling the "Tags" or
"Edit Metadata" toolbar (View > Cell Toolbar > Tags; View > Cell Toolbar >
Edit Metadata).

For the tags approach, enter `keep_output` in the text field for each desired cell, and
press the "Add tag" button. For the metadata approach, press the "Edit Metadata" button
on each desired cell, and edit the metadata to look like this:

```json
{
  "keep_output": true
}
```

This will tell the hook not to strip the resulting output of the desired cell(s),
allowing the output(s) to be committed.

```{note} Tags and metadata on Google Colab
Currently (March 2020) there is no way to add tags and/or metadata to Google Colab
notebooks.

It's strongly suggested that you download the Colab as a .ipynb file, and edit tags
and/or metadata using Jupyter before committing the code if you want to keep some
outputs.
```

[detect-secrets]: https://github.com/Yelp/detect-secrets
[detect-secrets-plugins]: https://github.com/Yelp/detect-secrets#currently-supported-plugins
[detect-secrets-keyword-detector]: https://github.com/Yelp/detect-secrets/blob/c59553fab7f657320cc998887e6c23a0c0794a2b/detect_secrets/plugins/keyword.py#L43
[nbstripout]: https://github.com/kynan/nbstripout
[isort]: https://github.com/pycqa/isort/
[black]: https://github.com/psf/black
[nbqa]: https://github.com/nbQA-dev/nbQA
[flake8]: https://github.com/PyCQA/flake8
[bandit]: https://github.com/PyCQA/bandit
[pre-commit]: https://pre-commit.com/
[pre-commit-install]: https://pre-commit.com/#install
