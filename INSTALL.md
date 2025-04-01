# Installing FIED

The Foundational Industry Energy Dataset (FIED) compiles and derives from
multiple data sources sources.
To provide transparency and reproducibility, we organized all the procedures, including obtaining the raw data and the processing steps, in a Python package named [`fied`](https://github.com/NREL/foundational-industry-energy-data). 

The first step to recreate FIED is to install the `fied` package as follows.

## Installation

There are multiple ways to install a Python package.
We strongly recommend using a virtual environment to avoid conflicts with other packages and projects in your system.
For development we use `pixi` and our recommended way to install `fied`.

Some options are:

1. [Installing with Pixi (recommended)](#installing-with-pixi-recommended)
2. [Installing with pip (simplest way)](#installing-with-pip)
3. [Installing with conda](#installing-with-conda)

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
### Installing with pip

Here we provide a Python package with the required
resources to obtain the used data and the procedures to process it.
the resources to access the required data
and the code to process it.
