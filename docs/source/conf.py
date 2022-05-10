# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import inspect
import os
import sys

import semantique

sys.path.insert(0, os.path.abspath('../../demo'))


# -- Project information -----------------------------------------------------

project = 'semantique'
copyright = '2021, Department of Geoinformatics – Z_GIS'
#author = 'Lucas van der Meer'

# The full version, including alpha/beta/rc tags
release = '0.1.0'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
  'sphinx.ext.autodoc',
  'sphinx.ext.autosummary',
  'sphinx.ext.intersphinx',
  'sphinx.ext.linkcode',
  'sphinx.ext.napoleon',
  'nbsphinx',
  'nbsphinx_link',
  'm2r2',
  'sphinxemoji.sphinxemoji'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['**.ipynb_checkpoints']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_book_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_theme_options = {
  "repository_url": "https://github.com/ZGIS/semantique",
  "repository_branch": "main",
  "path_to_docs": "docs/source",
  "use_edit_page_button": True,
  "use_repository_button": True,
  "use_issues_button": True,
  "launch_buttons": {
    "binderhub_url": "https://mybinder.org/v2/gh/ZGIS/semantique/HEAD?labpath=demo"
  }
}

# -- Napoleon configuration ---------------------------------------------------

napoleon_use_param = False
napoleon_use_rtype = False
napoleon_preprocess_types = True
napoleon_type_aliases = {
    # objects without namespace
    "QueryRecipe": "~semantique.QueryRecipe",
    "Factbase": "~semantique.factbase.Factbase",
    "Ontology": "~semantique.ontology.Ontology",
    "SpatialExtent": "~semantique.extent.SpatialExtent",
    "TemporalExtent": "~semantique.extent.TemporalExtent",
    "QueryProcessor": "~semantique.processor.core.QueryProcessor",
    "Cube": "~semantique.processor.structures.Cube",
    "CubeCollection": "~semantique.processor.structures.CubeCollection",
    "create_extent_cube": "~semantique.processor.utils.create_extent_cube"
}

# -- Autosummary configuration ------------------------------------------------

autosummary_generate = True

# -- Nbsphinx configuration ---------------------------------------------------

nbsphinx_prolog = """
{% set docname = env.doc2path(env.docname, base=None) %}
Explore this notebook interactively: |Binder|.

.. |Binder| image:: https://mybinder.org/badge.svg
   :target: https://mybinder.org/v2/gh/ZGIS/semantique/HEAD?labpath=demo%2F{{ docname }}
"""

# -- Intersphinx configuration ------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "xarray": ("https://xarray.pydata.org/en/stable/", None),
    "rioxarray": ("https://corteva.github.io/rioxarray/stable/", None),
    "geopandas": ("https://geopandas.org/en/stable/", None),
    "pyproj": ("https://pyproj4.github.io/pyproj/stable/", None),
    "datacube": ("https://datacube-core.readthedocs.io/en/latest/", None)
}

# -- Linkcode configuration ---------------------------------------------------

# based on xarray doc/conf.py
def linkcode_resolve(domain, info):
  """Determine the URL corresponding to Python object."""
  if domain != "py":
      return None
  modname = info["module"]
  fullname = info["fullname"]
  submod = sys.modules.get(modname)
  if submod is None:
      return None
  obj = submod
  for part in fullname.split("."):
      try:
          obj = getattr(obj, part)
      except AttributeError:
          return None
  try:
      fn = inspect.getsourcefile(inspect.unwrap(obj))
  except TypeError:
      fn = None
  if not fn:
      return None
  try:
      source, lineno = inspect.getsourcelines(obj)
  except OSError:
      lineno = None
  if lineno:
      linespec = f"#L{lineno}-L{lineno + len(source) - 1}"
  else:
      linespec = ""
  fn = os.path.relpath(fn, start = os.path.dirname(semantique.__file__))
  return f"https://github.com/ZGIS/semantique/blob/main/semantique/{fn}{linespec}"