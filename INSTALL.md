# Installing FIED

The Foundational Industry Energy Dataset (FIED) compiles and derives from
multiple data sources in a well defined sequence of steps.
To provide transparency and reproducibility, we organized all the procedures, including obtaining the raw data and the processing steps, in a Python package named [`fied`](https://github.com/NREL/foundational-industry-energy-data). 

**Why the trouble of creating a Python package?** We could just share the used scripts. However, our scripts are built on top of other Python packages that change with time. Actually, we evolve as well. For instance, to extend from the 2017 to the 2020 edition we had to adapt to changes in the data sources as well as we improved some steps on our procedures.
By providing a formal Package, we can specify the required dependencies and make it easier for anyone to reproduce our results, as well as re-use our resources and extend for other purposes.
Also, by tagging the versions of the package, we can keep track of the changes and be able to reproduce the results at any time.

The first step to recreate FIED is to install the `fied` package as follows.

## Installation

There are several ways to install a Python package.
We strongly recommend using a virtual environment to avoid conflicts with other packages and projects in your system.
For development we use `pixi` and our recommended way to install `fied`.

Some options are:

1. [Installing with PIP](#installing-with-pip)
2. [Installing with Pixi (recommended)](#installing-with-pixi-recommended)
3. [Installing with Conda](#installing-with-conda)

### Installing with PIP

Assuming that you already have Python installed in your system, you most probably have `pip` as well.
To confirm that you have Python and PIP installed and learn the versions you have, you can run

   ```bash
   python3 --version
   ```

   and

   ```bash
   python3 -m pip --version
   ```

With that, you can use `pip` to install the `fied` package directly from our repository, i.e. the latest version.

   ```bash
   pip install -U git+https://github.com/NREL/foundational-industry-energy-data.git
   ```

Now you can run it on a terminal or use it inside Python.
In a terminal, you can run

   ```bash
   fied
   ```

and in Python import it and re-use as a regular package

   ```python
   import fied
   print(fied.__version__)
   ```

### Installing with Pixi (recommended)


We recommend using `pixi` to manage a Python environment with `fied` and all the required dependencies since it can reproduce exactly what we use for development.
Other alternatives are to install `fied` with `pip` or `conda`.


1. Install `pixi` itself on your system. You only need to do this once, so you can skip this step if you already have `pixi` installed.

Please follow the instructions at [pixi.sh](https://pixi.sh) to install `pixi` on your system.

For Linux and MacOS:
   ```bash
   curl -fsSL https://pixi.sh/install.sh | sh
   ```

For Windows:
   ```cmd
   powershell -ExecutionPolicy ByPass -c "irm -useb https://pixi.sh/install.ps1 | iex"
   ```

2. Clone FIED repository if you don't already have it.

    ```bash
    git clone 

3. Pixi provides support to use virtual environment with all the dependencies
   required to run `fied`. To do so, pixi uses the information from
   `pyproject.toml` and `pixi.lock` files, therefore, you need to move to
   where you cloned the repository. Once there, you can activate your
   environment by running:
   ```bash
   pixi shell
   ```

   You might notice a `(fied)` prefix in your terminal, which indicates that
   you are now in the `fied` environment.
   Now you can run the command `fied --version` to verify which version you
   are using, and run `fied` to rebuild the dataset locally.

### Installing with Conda
