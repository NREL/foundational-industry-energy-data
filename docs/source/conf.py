# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'The Foundational Industry Energy Dataset (FIED)'
copyright = '2024, Alliance for Sustainable Energy, LLC'
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
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.githubpages',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon'
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'plotly': ('https://plot.ly/python/', None)
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
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True
