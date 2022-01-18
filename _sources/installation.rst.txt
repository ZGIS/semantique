Installation
=============

At this moment the package can only be installed from source. This can be done in several ways:

1) Using pip to install directly from GitHub:

.. code-block::

  pip install git+https://github.com/ZGIS/semantique.git

2) Cloning the repository first and then install with pip:

.. code-block::

  git clone https://github.com/ZGIS/semantique.git
  cd semantique
  pip install .

3) If you prefer to use conda, you can create a `semantique` conda environment with the package itself and all dependencies installed, using the provided `environment.yml` file:

.. code-block::

  git clone https://github.com/ZGIS/semantique.git
  cd semantique
  conda env create -f environment.yml
  conda activate semantique
