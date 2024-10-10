# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'The Foundational Industry Energy Dataset (FIED)'
copyright = '2024, Alliance for Sustainable Energy, LLC.'
author = 'NREL: Colin McMillan and Carrie Schoeneberger'
release = '0.1'

#-- Path setup --------------------------------------------------------------
import os
import sys
sys.path.insert(0, os.path.abspath('../../'))

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.githubpages',
    'sphinx.ext.viewcode'
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/version/1.2.0/', None),
    'numpy': ('https://numpy.org/doc/1.23/reference/index.html', None)
}

templates_path = ['_templates']
exclude_patterns = [
    "**.ipynb_checkpoints",
    "**__pycache__**",
    "**/includes/**"
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'press'
html_static_path = ['_static']

# -- Extension configuration -------------------------------------------------

autosummary_generate = True  # Turn on sphinx.ext.autosummary
autoclass_content = "both"  # Add __init__ doc (ie. params) to class summaries
autodoc_member_order = 'bysource'
autodoc_inherit_docstrings = True  # If no docstring, inherit from base class
add_module_names = False  # Remove namespaces from class/method signatures