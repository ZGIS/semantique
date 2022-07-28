# Semantic querying in Earth observation data cubes <img src="docs/_images/logo.png" align="right" width="120" />

[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ZGIS/semantique/HEAD?labpath=demo%2Ftest.ipynb)

`semantique` is a Python package that implements a structured framework for semantic querying in Earth observation data cubes, meaning that users can query the cube through an ontology. The package is under active development. The ideas and concepts behind the package are published in an academic paper, which we will link here as soon as the pre-print is available.

## Installation

At this moment the package can only be installed from source. This can be done in several ways:

1) Using pip to install directly from GitHub:

```
pip install git+https://github.com/ZGIS/semantique.git
```

2) Cloning the repository first and then install with pip:

```
git clone https://github.com/ZGIS/semantique.git
cd semantique
pip install .
```

3) If you prefer to use conda, you can create a `semantique` conda environment with the package itself and all dependencies installed, using the provided [environment.yml](environment.yml) file:

```
git clone https://github.com/ZGIS/semantique.git
cd semantique
conda env create -f environment.yml
conda activate semantique
```

## Usage

The package has a [documentation website](https://zgis.github.io/semantique/index.html) describing all its functionalities. It contains an [API reference](https://zgis.github.io/semantique/reference.html), as well as a [User guide](https://zgis.github.io/semantique/guide.html) consisting several notebooks that combine text with code chunks.

To explore the package in an interactive way you can make use of [Binder](https://mybinder.org/). Simply click on the *launch binder* badge at the top of this README. Doing so will setup an online environment for you with the package and all its dependencies installed. No installations or complicated system configurations required, you only need a web browser! Inside the environment you will also have access to a tiny demo data cube containing a few resources of data meant for testing purposes. Once the environment is build (it make take a few minutes), you will see a Jupyter Notebook which you can use as a starting point for the exploration. You can also create your own notebooks, or adapt the existing documentation notebooks mentioned above. Happy coding!

## Contribution

Contributions of any kind are very welcome! Please see the [contributing guidelines](CONTRIBUTING.md).

## Acknowledgements

This is developed with support by the Austrian Research Promotion Agency (FFG) under the Austrian Space Application Programme (ASAP) within the projects [Sen2Cube.at](https://projekte.ffg.at/projekt/2975644), [SemantiX](https://projekte.ffg.at/projekt/3769928) and [SIMS](https://projekte.ffg.at/projekt/4052529).

## Copyright

Copyright 2021 Department of Geoinformatics – Z_GIS

Licensed under the Apache License, Version 2.0 (the "License"). You may not use any file in the source code of this package except in compliance with the License. You may obtain a copy of the License at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
