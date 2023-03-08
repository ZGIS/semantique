.. currentmodule:: semantique

API Reference
##############

The API Reference contains auto-generated function documentation. This kind of documentation is meant to be concise and more technically oriented. The functions are grouped into different categories. As a regular user, you will mainly use the functions in the **Components** and **Building Blocks** categories to build and execute query recipes within a specific context. The functions in the **Processor** category are those internally used for query processing, and mainly of interest for advanced users and developers. The **Exceptions** category contains the custom exception classes of semantique.

Components
===========

Recipe
-------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   QueryRecipe

Mapping
---------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   mapping.Mapping
   mapping.Semantique

EO data cube
-------------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   datacube.Datacube
   datacube.Opendatacube
   datacube.GeotiffArchive

Spatio-temporal extent
-----------------------

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
   layer
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

Verbs for single arrays
~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   ArrayProxy.evaluate
   ArrayProxy.extract
   ArrayProxy.filter
   ArrayProxy.filter_time
   ArrayProxy.filter_space
   ArrayProxy.assign
   ArrayProxy.assign_time
   ArrayProxy.assign_space
   ArrayProxy.groupby
   ArrayProxy.groupby_time
   ArrayProxy.groupby_space
   ArrayProxy.reduce
   ArrayProxy.shift
   ArrayProxy.smooth
   ArrayProxy.trim
   ArrayProxy.delineate

Verbs for array collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   CollectionProxy.compose
   CollectionProxy.concatenate
   CollectionProxy.merge
   CollectionProxy.evaluate
   CollectionProxy.extract
   CollectionProxy.filter
   CollectionProxy.filter_time
   CollectionProxy.filter_space
   CollectionProxy.assign
   CollectionProxy.assign_time
   CollectionProxy.assign_space
   CollectionProxy.reduce
   CollectionProxy.shift
   CollectionProxy.smooth
   CollectionProxy.trim
   CollectionProxy.delineate

Utility verbs
~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   ArrayProxy.name

Special values
---------------

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   label
   set
   interval
   geometry
   time_instant
   time_interval

Dimensions
-----------

Reserved dimension names
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   dimensions.TIME
   dimensions.SPACE
   dimensions.X
   dimensions.Y

Dimension components
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   components.time.YEAR
   components.time.SEASON
   components.time.QUARTER
   components.time.MONTH
   components.time.WEEK
   components.time.DAY_OF_WEEK
   components.time.DAY_OF_YEAR
   components.time.HOUR
   components.time.MINUTE
   components.time.SECOND
   components.space.X
   components.space.Y
   components.space.FEATURE

Operators
----------

Univariate operators
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   operators.NOT
   operators.IS_MISSING
   operators.NOT_MISSING
   operators.ABSOLUTE
   operators.EXPONENTIAL
   operators.CUBE_ROOT
   operators.NATURAL_LOGARITHM
   operators.SQUARE_ROOT
   operators.FLOOR
   operators.CEILING

Algebraic operators
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   operators.ADD
   operators.DIVIDE
   operators.MULTIPLY
   operators.POWER
   operators.SUBTRACT
   operators.NORMALIZED_DIFFERENCE

Boolean operators
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   operators.AND
   operators.OR
   operators.EXCLUSIVE_OR

Relational operators
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   operators.EQUAL
   operators.NOT_EQUAL
   operators.IN
   operators.NOT_IN
   operators.GREATER
   operators.GREATER_EQUAL
   operators.LESS
   operators.LESS_EQUAL


Spatial relational operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   operators.INTERSECTS

Temporal relational operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   operators.AFTER
   operators.BEFORE
   operators.DURING

Reducers
---------

Statistical reducers
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   reducers.MEAN
   reducers.MEDIAN
   reducers.MODE
   reducers.MAX
   reducers.MIN
   reducers.RANGE
   reducers.N
   reducers.PRODUCT
   reducers.STANDARD_DEVIATION
   reducers.SUM
   reducers.VARIANCE

Boolean reducers
~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   reducers.ALL
   reducers.ANY
   reducers.NONE

Occurence reducers
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   reducers.COUNT
   reducers.PERCENTAGE

Positional reducers
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   reducers.FIRST
   reducers.LAST

Processor
==========

Core
-----

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.core.QueryProcessor

Array objects
--------------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   processor.arrays.Array
   processor.arrays.Collection

Special value objects
----------------------

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   processor.values.Interval

Operator functions
-------------------

Univariate operators
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.not_
   processor.operators.is_missing_
   processor.operators.not_missing_
   processor.operators.absolute_
   processor.operators.exponential_
   processor.operators.cube_root_
   processor.operators.natural_logarithm_
   processor.operators.square_root_
   processor.operators.floor_
   processor.operators.ceiling_

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
   processor.operators.normalized_difference_

Boolean operators
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.and_
   processor.operators.or_
   processor.operators.exclusive_or_

Relational operators
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.operators.equal_
   processor.operators.not_equal_
   processor.operators.in_
   processor.operators.not_in_
   processor.operators.greater_
   processor.operators.greater_equal_
   processor.operators.less_
   processor.operators.less_equal_


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
   processor.reducers.none_

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

.. autosummary::
   :toctree: _generated/
   :nosignatures:

   processor.utils.parse_extent
   processor.utils.parse_datetime_component
   processor.utils.get_null
   processor.utils.allnull
   processor.utils.null_as_zero
   processor.utils.inf_as_null
   processor.utils.datetime64_as_unix
   processor.utils.convert_datetime64

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
   exceptions.InvalidIntervalError
   exceptions.InvalidBuildingBlockError
   exceptions.UnknownConceptError
   exceptions.UnknownLayerError
   exceptions.UnknownResultError
   exceptions.UnknownReducerError
   exceptions.UnknownOperatorError
   exceptions.UnknownDimensionError
   exceptions.UnknownComponentError
   exceptions.UnknownLabelError
   exceptions.TooManyDimensionsError
   exceptions.MissingDimensionError
   exceptions.MixedDimensionsError
   exceptions.ReservedDimensionError
   exceptions.MixedTimeZonesError