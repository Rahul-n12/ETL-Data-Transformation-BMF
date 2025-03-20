# Migration Specialist technical Project

## Introduction
This project simulates a normal day for a Migration Specialist. A customer needs to migrate their data from a source system into a target system. They don't know exactly how the data should be transformed to suit the new system, and it is our job to figure out how to transform their data so it fits the new structure.

They have already extracted their data from their source system into excel and transferred it to us (it is the data set `input/input.xslx`).

For us to be able to load the data into the new system, we need to transform this to a table having a specific format and with columns following specific rules. Since we are experts in the system we are implementing, we know how the final data needs to look like to fit into the system (this is the file `outputs/solution.csv`).

The goal of this project is to transform `input/input.xslx` into a file that matches `outputs/solution.csv`.

**The BASE Migration Framework**

Because of the importance of correctness of the data we are migrating, we need to log, document and test every transformation step along the way - this means that we are not allowed to simply make the transformations in an untested Jupyter Notebook. Because of this, we have built our own framework `BASE Migration Framework` to assure we are logging and documenting everything.

The folder `bmf` is a simulation of this Python package containing the bare minimum for doing the necessary transformations.

Along the way when delivering a migration to a customer, we usually find, that we need to implement new transformations in our framework. Therefore the `bmf` is limited in the types of transformations it can do, and it will need to be extended with new transformation functions.

## Directory content

The directory contains the following files:
```
├── bmf                        <- A minimal simulation of the BASE Migration Framework.
│   │
│   ├── bmf                    <- The bmf package.
│   │    ├── engine.py         <- The transformation engine.
│   │    ├── functions.py      <- The transormation functions that the engine can apply.
│   │    └── storage.py        <- Storage class for reading and writing files.
│   ├── README.md              <- BASE Migration Framework readme.
│   └── setup.py               <- Setup file for pip package installation.
│
├── input
│   └── input.xlsx             <- The input file.
│
├── outputs                    <- Folder containing output files.
│   └── solution.csv           <- The solution file.
│
├── Makefile                   <- The Makefile containing the installation for Linux/UNIX.
├── README.md                  <- This file.
├── transform.py               <- The BASE Migration Framework execution file.
└── transformation_config.yml  <- The configuration file for the transformation engine.
```

The file `transformation_config.yml` is the YAML configuration file, which configures what transformations/actions (i.e. which functions in `bmf/bmf/functions.py`) the engine should apply to the input data to transform it into the output data.

## Installing and setting up

For Linux/UNIX (incl. macOS), please see `intallation_linux.md`.

For Windows, please see `installation_windows.md`.

## Implementation

The command

```bash
python transform.py -c transformation_config.yml
```

creates the file `outputs/output.csv`. The task is to make this output file to be exactly the same as the file `outputs/solution.csv` by only adding to the two files:

- transformation_config.yml
- bmf/bmf/functions.py


**Focus**

Because of the significance of high data quality, the primary focus is to make the `outputs/output.csv` file match `outputs/solution.csv` with 100% accuracy.

The second focus is on readability, documentation and extendibility of both the python code and yaml configuration implemented to solve this task.

**Delivery**

The delivery should be a zip-file containing the modified files, so that the `outputs/output.csv` file created by the command
```
python transform.py -c transformation_config.yml
```
matches `outputs/solution.csv`.

Please remember to delete the `venv` folder prior to zipping.

## Extra
In case of extra time, one could consider (and implement) solutions to one or more of the following:

- How can we expand the transformation engine so we can define a filter/condition to only apply transformations on a subset of the rows?
- How can we implement logging of transformations and configurations to a file?
- What types of validation can we implement on the output table to assure correctness?
