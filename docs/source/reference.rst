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
   result
   self
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
   value_range
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

Data cube objects
------------------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   processor.structures.Cube
   processor.structures.CubeCollection

Special value objects
----------------------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   processor.values.ValueRange

Operator functions
-------------------

Numerical univariate operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.absolute_
   processor.operators.exponential_
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

Spatial relational operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.intersects_

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

Statistical reducers
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.mean_
   processor.reducers.median_
   processor.reducers.mode_
   processor.reducers.max_
   processor.reducers.min_
   processor.reducers.range_
   processor.reducers.n_
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

Occurence reducers
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.count_
   processor.reducers.percentage_

Positional reducers
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.reducers.first_
   processor.reducers.last_

Utils
------

Utility functions to interact with xarray
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.utils.create_extent_cube
   processor.utils.parse_datetime_component
   processor.utils.parse_coords_component

Utility functions to interact with numpy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.utils.convert_datetime64
   processor.utils.np_null
   processor.utils.np_allnull
   processor.utils.np_null_as_zero
   processor.utils.np_inf_as_null

Value type handling
-------------------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.types.TypePromoter
   processor.types.get_value_type
   processor.types.get_value_labels
   processor.types.DTYPE_MAPPING
   processor.types.TYPE_PROMOTION_MANUALS


Exceptions
===========

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   exceptions.AlignmentError
   exceptions.EmptyDataError
   exceptions.InvalidValueTypeError
   exceptions.InvalidValueRangeError
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