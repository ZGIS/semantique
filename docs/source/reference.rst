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
   :template: dictlike.rst
   :nosignatures:

   QueryRecipe

Ontology
---------

.. autosummary::
   :toctree: _generated/
   :template: dictlike.rst
   :nosignatures:

   ontology.Ontology
   ontology.Semantique

Factbase
---------

.. autosummary::
   :toctree: _generated/
   :template: dictlike.rst
   :nosignatures:

   factbase.Factbase
   factbase.Opendatacube
   factbase.GeotiffArchive

Extent
-------

.. autosummary::
   :toctree: _generated/
   :template: dictlike.rst
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
   CubeProxy.replace

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
   CubeCollectionProxy.replace

Special values
---------------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   category
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
   :nosignatures:

   processor.structures.Cube
   processor.structures.CubeCollection

Operator functions
-------------------

Algebraic unary operators
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.absolute_
   processor.operators.cube_root_
   processor.operators.natural_logarithm_
   processor.operators.square_root_

Algebraic binary operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.add_
   processor.operators.divide_
   processor.operators.multiply_
   processor.operators.power_
   processor.operators.subtract_

Boolean unary operators
~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.invert_

Boolean binary operators
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.and_
   processor.operators.or_

Regular relational operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
   processor.reducers.count_
   processor.reducers.percentage_

Universal reducers
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.first_
   processor.reducers.last_
   processor.reducers.max_
   processor.reducers.median_
   processor.reducers.min_
   processor.reducers.mode_

Utils
------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.utils.convert_datetime64
   processor.utils.create_extent_cube

Templates
----------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.templates.TYPE_PROMOTION_TEMPLATES

Exceptions
===========

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   exceptions.AlignmentError
   exceptions.EmptyDataError
   exceptions.InvalidTypePromotionError
   exceptions.InvalidReferenceError
   exceptions.InvalidBuildingBlockError
   exceptions.UnknownReducerError
   exceptions.UnknownOperatorError
   exceptions.UnknownGeometryTypeError
   exceptions.MixedTimeZonesError
   exceptions.MixedValueTypesError
   exceptions.TooManyDimensionsError
   exceptions.MissingDimensionError
   exceptions.UnmatchingDimensionsError
   exceptions.UndefinedDimensionComponentError
   exceptions.UndefinedCategoryLabelError