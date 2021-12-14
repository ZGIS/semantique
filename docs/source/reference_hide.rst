.. Generate API reference pages, but don't display these in tables.
.. This extra page is a work around for sphinx not having any support for
.. hiding an autosummary table.

:orphan:

.. currentmodule:: semantique

.. autosummary::
   :toctree: _generated/
   :template: dictlike.rst
   :nosignatures:

   CubeProxy
   CubeCollectionProxy

.. autosummary::
   :toctree: _generated/

   extent.SpatialExtent.crs
   extent.SpatialExtent.from_geojson
   extent.SpatialExtent.from_featurecollection
   extent.SpatialExtent.from_feature
   extent.SpatialExtent.from_geometry
   extent.SpatialExtent.rasterize

   extent.TemporalExtent.tz
   extent.TemporalExtent.start
   extent.TemporalExtent.end
   extent.TemporalExtent.discretize