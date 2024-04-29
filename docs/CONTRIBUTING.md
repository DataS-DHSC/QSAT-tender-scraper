# Contributing

We love contributions! We've compiled this documentation to help you understand our
contributing guidelines. [If you still have questions, please contact us][email] and
we'd be happy to help!

## Code of Conduct

[Please read `CODE_OF_CONDUCT.md` before contributing](CODE_OF_CONDUCT.md).

## Getting started

To start contributing, open your terminal, navigate to the root directory and run 

```shell
bash setup_project.bat
```
This will perform a number of steps for you, including:
* Setting up a virtual environment 
* Installing pre-commit hooks 
* Initalising a git repository if one doesn't already exist


The pre-commit hooks are a security feature to ensure, for example, no secrets[^1],
or Jupyter notebook outputs are accidentally committed into the
repository. [For more information on pre-commit hooks see our
documentation](user_guide/pre_commit_hooks.md)

[^1]: [Only secrets of specific patterns are detected by the pre-commit
      hooks](user_guide/pre_commit_hooks.md#definition-of-a-secret-according-to-detect-secrets).


### Git and GitHub

We use Git to version control the source code. [Please read the Quality assurance of code for analysis and research for details on Git best practice][duck-book-version-control]. This includes how to write good commit messages, how to branch correctly and solving merge conflicts.

[If you want to modify the `.gitignore` files, see the template
documentation](user_guide/updating_gitignore.md) for further details.

Our source code is stored on GitHub. Pull requests into `main` require at least one
approved review.

### Python

We use [Black][black] to format our code. Black is an opinionated formatter that follows PEP 8; where Black and PEP 8 do not express a view (for example, on the usage of language features such as metaclasses) we defer to the [Google Python style guide][google-python-style-guide]. 


### Markdown

Local links can be written as normal, but external links should be referenced at the
bottom of the Markdown file for clarity. For example:

Use a [local link to reference the `README.md`](../../README.md) file, but [an external
link for GOV.UK][gov-uk].

We also try to wrap Markdown to a line length of 88 characters, but this is not
strictly enforced in all cases, for example with long hyperlinks.


[black]: https://github.com/psf/black
[code-of-conduct]: https://github.com/best-practice-and-impact/govcookiecutter/blob/main/%7B%7B%20cookiecutter.repo_name%20%7D%7D/docs/contributor_guide/CODE_OF_CONDUCT.md
[duck-book-version-control]: https://best-practice-and-impact.github.io/qa-of-code-guidance/version_control.html
[pre-commit]: https://pre-commit.com 
[gov-uk]: https://www.gov.uk/
[google-python-style-guide]: https://google.github.io/styleguide/pyguide.html
[email]: mailto:george.jenkins@dhsc.gov.uk
