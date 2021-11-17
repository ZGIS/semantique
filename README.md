[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ZGIS/semantique/HEAD?labpath=demo%2Ftest.ipynb)

# Semantic querying of EO data

`semantique` is a Python package that implements a structured framework for semantic querying of earth observation data. It is currently in a phase of active development. No stable release exists up to now.

## Installation

At this moment the package can only be installed from source. This can be done in two ways:

1) Using pip to install directly from GitHub

```
pip install git+https://github.com/ZGIS/semantique.git
```

2) Cloning the repository first and then install with pip

```
git clone https://github.com/ZGIS/semantique.git
cd semantique
pip install .
```

## Usage

There are several notebooks available that present the functionalities of the package:

- [Intro](demo/intro.ipynb): An overall introduction to the package
- [References](demo/references.ipynb): A detailed explanation of all data cube references that can be used in the queries.
- [Verbs](demo/verbs.ipynb): A detailed explanation of the data cube specific processes that can be used in the queries
- [Advanced usage](demo/advanced.ipynb): Insights in the internal query processing workflow and guidelines for advanced users
- [Gallery](demo/gallery.ipynb): Some examples of queries and their responses

To explore the package in an interactive way you can make use of [Binder](https://mybinder.org/). Simply click on the *launch binder* badge at the top of this README. Doing so will setup an online environment for you with the package and all its dependencies installed. No installations or complicated system configurations required, you only need a web browser! Inside the environment you will also have access to a tiny demo data cube containing a few resources of data meant for testing purposes. Once the environemnt is build (it make take a few minutes), you will see a Jupyter Notebook which you can use as a starting point for the exploration. You can also create your own notebooks, or adapt the existing documentation notebooks mentioned above. Happy coding!

## Acknowledgements

This is developed with support by the Austrian Research Promotion Agency (FFG) under the Austrian Space Application Programme (ASAP) within the project [Sen2Cube.at](http://sen2cube.at) (project no.: 866016).

## Legal info

Copyright 2021 Department of Geoinformatics – Z_GIS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.