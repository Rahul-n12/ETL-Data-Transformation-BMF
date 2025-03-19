# Installation on Windows

The assignment requires python3.7.

To create a virtual environment and install the `bmf` package in editable mode, run the commands:

```powershell
python3.7 -m venv venv
venv\Scripts\python -m pip install --upgrade pip setuptools
venv\Scripts\python -m pip install -e ./bmf
venv\Scripts\python bmf/setup.py clean
```

This will make the virtual environment, which can then be activated 
with
```powershell
.\venv\Scripts\activate
```

From then, the command
```
python transform.py -c transformation_config.yml
```
should create the file `outputs/output.csv`. If that is successful, the environment is set up correctly.
