[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ZGIS/semantique/HEAD?labpath=demo%2Ftest.ipynb)

# Semantic querying of EO data

`semantique` is a Python package that implements a structured framework for semantic querying of earth observation data. The package is under active development, and no stable release currently exists.

## Background

Earth observation (EO) imagery has become an essential source of information to better monitor and understand the impact of major social and environmental issues. In recent years we have seen significant improvements in availability and accessibility of these data. Programs like Landsat and Copernicus release new images every day, freely and openly available to everyone. Technological improvements such as data cubes (e.g. OpenDataCube), scalable cloud-based analysis platforms (e.g. Google Earth Engine) and standardized data access APIs (e.g. OpenEO) are easing the retrieval of the data and enabling higher processing speeds.

All these developments have lowered the barriers for utilizing the value of EO imagery, yet translating EO imagery directly into information using automated and repeatable methods remains a main challenge. Imagery lacks inherent semantic meaning, thus requires interpretation. For example, consider someone who uses EO imagery to monitor vegetation loss. A multi-spectral satellite image of a location may consist of an array of digital numbers representing the intensity of reflected radiation at different wavelengths. The user, however, is not interested in digital numbers, they are interested in a semantic categorical value stating if vegetation was observed. Inferring this semantic variable from the reflectance values is an inherently ill-posed problem, since it requires bridging a gap between the two-dimensional image domain and the four-dimensional spatio-temporal real-world domain. Advanced technical expertise in the field of EO analytics is needed for this task, making it a remaining barrier on the way to a broad utilization of EO imagery across a wide range of application domains.

We propose a semantic querying framework for extracting information from EO imagery as a tool to help bridge the gap between imagery and semantic concepts. The novelty of this framework is that it makes a clear separation between the image domain and the real-world domain.

There are three main components in the framework. The first component forms the real-world domain. This is where EO data users interact with the system. They can express their queries in the real-world domain, meaning that they directly reference semantic concepts that exist in the real world (e.g. forest, fire). For simplicity reasons, we currently work on a higher level of abstraction, and focus on concepts that correspond to land-cover classes (e.g. vegetation). For example, a user can query how often vegetation was observed at a certain location during a certain timespan. These queries do not contain any information on how the semantic concepts are represented by the underlying data.

The second component forms the image domain. This is where the EO imagery is stored in a data cube, a multi-dimensional array organizing the data in a way that simplifies storage, access and analysis. Besides the imagery itself, the data cube may be enriched with automatically generated layers that already offer a first degree of interpretation for each pixel (i.e. a semantically-enabled data cube), as well as with additional data sources that can be utilized to better represent certain properties of real-world semantic concepts (e.g. digital elevation models).

The third component serves as the mapping between the real-word domain and the image domain. This is where EO data experts bring their expertise into the system, by formalizing relationships between the observed data values and the presence of a real-world semantic concept. In our current work these relationships are always binary, meaning that the concept is marked either as present or not present. However, the structure allows also for non-binary relationships, e.g. probabilities that a concept is present given the observed data values.

The `semantique` package is the proof-of-concept implementation of our proposed framework. It contains functions and classes that allow users to formulate their queries and call a query processor to execute them with respect to a specific mapping. Queries are formulated by chaining together semantic concept references and analytical processes. The query processor will translate each referenced semantic concept into a multi-dimensional array covering the spatio-temporal extent of the query. It does so by retrieving the relevant data values from the data storage, and subsequently applying the rules that are specified in the mapping. If the relationships are binary, the resulting array will be boolean, with “true” values for those pixels that are identified as being an observation of the referenced concept, and “false” values for all other pixels. Analytical processes can then be applied to this array. Each process is a well-defined array operation performing a single task. For example, applying a function to each pixel or reducing a particular dimension. The workflow of chaining together different building blocks can easily be supported by a visual programming interface, and thus lowering the technical barrier for information extraction even more. This is demonstrated already in an operational setting by [Sen2Cube.at](https://sen2cube.at/), a nation-wide semantic data cube infrastructure for Austria, which uses `semantique` in the background.

We believe our proposed framework is an important contribution to more widely accessible EO imagery. It lowers the barrier to extract valuable information from EO imagery for users that lack the advanced technical knowledge of EO data, but can benefit from the applications of it in their specific domain. They can now formulate queries by directly referencing real-world semantic concepts, without having to formalize how they are represented by the EO data. To execute the queries, they can use pre-defined mappings, which are application-independent and shareable.  The framework eases interoperability of EO data analysis workflows also for expert users. Mappings can easily be shared and updated, and the queries themselves are robust against changes in the image domain.

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

There are several notebooks available that present the functionalities of the package:

- [Intro](demo/intro.ipynb): An overall introduction to the package
- [References](demo/references.ipynb): A detailed explanation of all data cube references that can be used in the queries.
- [Verbs](demo/verbs.ipynb): A detailed explanation of the data cube specific processes that can be used in the queries
- [Advanced usage](demo/advanced.ipynb): Insights in the internal query processing workflow and guidelines for advanced users
- [Gallery](demo/gallery.ipynb): Some examples of queries and their responses

To explore the package in an interactive way you can make use of [Binder](https://mybinder.org/). Simply click on the *launch binder* badge at the top of this README. Doing so will setup an online environment for you with the package and all its dependencies installed. No installations or complicated system configurations required, you only need a web browser! Inside the environment you will also have access to a tiny demo data cube containing a few resources of data meant for testing purposes. Once the environment is build (it make take a few minutes), you will see a Jupyter Notebook which you can use as a starting point for the exploration. You can also create your own notebooks, or adapt the existing documentation notebooks mentioned above. Happy coding!

## Contribution

Contributions of any kind are very welcome! Please see the [contributing guidelines](CONTRIBUTING.md).

## Acknowledgements

This is developed with support by the Austrian Research Promotion Agency (FFG) under the Austrian Space Application Programme (ASAP) within the projects [Sen2Cube.at](https://projekte.ffg.at/projekt/2975644) and [SemantiX](https://projekte.ffg.at/projekt/3769928).

## Copyright

Copyright 2021 Department of Geoinformatics – Z_GIS

Licensed under the Apache License, Version 2.0 (the "License"). You may not use any file in the source code of this package except in compliance with the License. You may obtain a copy of the License at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
