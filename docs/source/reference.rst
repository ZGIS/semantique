.. currentmodule:: semantique

API Reference
##############

The API Reference contains auto-generated function documentation. This kind of documentation is meant to be concise and more technically oriented. The functions are grouped into different categories. As a regular user, you will mainly use the functions in the **Components** and **Building Blocks** categories, to prepare the different components of a semantic query, and to build and execute query recipes. The functions in the **Processor** category are those internally used for query processing, and mainly of interest for advanced users and developers. The **Exceptions** category contains the custom exception classes of semantique.

Components
===========

Recipe
-------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   QueryRecipe

Ontology
---------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   ontology.Ontology
   ontology.Semantique

Factbase
---------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   factbase.Factbase
   factbase.Opendatacube
   factbase.GeotiffArchive

Extent
-------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   extent.SpatialExtent
   extent.TemporalExtent

Building blocks
================

References
-----------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   concept
   entity
   event
   resource
   appearance
   artifacts
   atmosphere
   reflectance
   topography
   collection

Verbs
------

Verbs for single data cubes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   CubeProxy.evaluate
   CubeProxy.extract
   CubeProxy.filter
   CubeProxy.filter_time
   CubeProxy.filter_space
   CubeProxy.groupby
   CubeProxy.groupby_time
   CubeProxy.groupby_space
   CubeProxy.label
   CubeProxy.reduce

Verbs for data cube collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   CubeCollectionProxy.compose
   CubeCollectionProxy.concatenate
   CubeCollectionProxy.merge
   CubeCollectionProxy.evaluate
   CubeCollectionProxy.extract
   CubeCollectionProxy.filter
   CubeCollectionProxy.filter_time
   CubeCollectionProxy.filter_space
   CubeCollectionProxy.label
   CubeCollectionProxy.reduce

Special values
---------------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   value_label
   geometries
   time_instant
   time_interval

Processor
==========

Core
-----

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.core.QueryProcessor

Data structures
----------------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   processor.structures.Cube
   processor.structures.CubeCollection

Operator functions
-------------------

Numerical univariate operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.absolute_
   processor.operators.cube_root_
   processor.operators.natural_logarithm_
   processor.operators.square_root_

Boolean univariate operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.invert_

Algebraic operators
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.add_
   processor.operators.divide_
   processor.operators.multiply_
   processor.operators.power_
   processor.operators.subtract_

Boolean operators
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.and_
   processor.operators.or_

Relational operators
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.equal_
   processor.operators.exclusive_or_
   processor.operators.greater_
   processor.operators.greater_equal_
   processor.operators.in_
   processor.operators.less_
   processor.operators.less_equal_
   processor.operators.not_equal_
   processor.operators.not_in_

Temporal relational operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.after_
   processor.operators.before_
   processor.operators.during_

Assignment operators
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.assign_

Reducer functions
------------------

Numerical reducers
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.mean_
   processor.reducers.product_
   processor.reducers.standard_deviation_
   processor.reducers.sum_
   processor.reducers.variance_

Boolean reducers
~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.all_
   processor.reducers.any_

Count reducers
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.count_
   processor.reducers.percentage_

Universal reducers
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.first_
   processor.reducers.last_
   processor.reducers.mode_

Ordered reducers
~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.max_
   processor.reducers.median_
   processor.reducers.min_

Utils
------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.utils.convert_datetime64
   processor.utils.create_extent_cube
   processor.utils.parse_datetime_component

Templates
----------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.templates

Exceptions
===========

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   exceptions.AlignmentError
   exceptions.EmptyDataError
   exceptions.InvalidValueTypeError
   exceptions.InvalidBuildingBlockError
   exceptions.UnknownConceptError
   exceptions.UnknownResourceError
   exceptions.UnknownResultError
   exceptions.UnknownReducerError
   exceptions.UnknownOperatorError
   exceptions.UnknownDimensionError
   exceptions.UnknownComponentError
   exceptions.UnknownLabelError
   exceptions.TooManyDimensionsError
   exceptions.MissingDimensionError
   exceptions.MixedDimensionsError
   exceptions.MixedTimeZonesError