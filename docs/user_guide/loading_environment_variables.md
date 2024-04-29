# Loading environment variables

[We use `python-dotenv` to load environment variables][python-dotenv], as these are only loaded when
inside the project folder. This can prevent accidental conflicts with identically named
variables. 

## Using `python-dotenv`

To load the environment variables, first make sure you have
python-dotenv install, and [make sure you have a `.env` file to store
secrets and credentials](../../.env). Then to load in the
environment variables into a python script see instructions in `.env` file.


## Storing secrets and credentials

Secrets and credentials must be stored in the `.env` file. This file is not
version-controlled, so no secrets should be committed to GitHub.

If there is no `.env` file in your project then create one.

In your terminal navigate to the root folder

```shell
touch .env
```

Open this new `.env` file using your preferred text editor, and add any secrets as
environmental variables. For example, to add a JSON credentials file for Google
BigQuery, save the following changes to `.env`.

```shell
GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
```

Once complete, load the `.env` file using:

```shell
from dotenv import load_dotenv
import os

#Load secrets from the `.env` file, overriding any system environment variables
load_dotenv(override=True)
#Example variable
EXAMPLE_VARIABLE = os.getenv("EXAMPLE_VARIABLE")
```

[python-dotenv]: https://saurabh-kumar.com/python-dotenv/
