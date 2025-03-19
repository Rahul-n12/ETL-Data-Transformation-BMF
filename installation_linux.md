# Installation on Linux/UNIX

The assignment requires python3.7.

> NOTE: On Ubuntu/Debian, the packages `python3.7-dev` and `python3.7-venv` need to be installed.

To create a virtual environment and install the `bmf` package in editable mode, run the command

```bash
make setup
```

This will make the virtual environment, which can then be activated with
```bash
source venv/bin/activate
```

From then, the command
```
python transform.py -c transformation_config.yml
```
should create the file `outputs/output.csv`. If that is successful, the environment is set up correctly.
