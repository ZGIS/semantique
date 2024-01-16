.. Generate API reference pages, but don't display these in tables.
.. This extra page is a work around for sphinx not having any support for
.. hiding an autosummary table.

:orphan:

.. currentmodule:: semantique

.. autosummary::
   :toctree: _generated/
   :template: inherited.rst
   :nosignatures:

   ArrayProxy
   CollectionProxy

.. autosummary::
   :toctree: _generated/

   QueryRecipe.execute

   mapping.Mapping.lookup
   mapping.Mapping.translate

   mapping.Semantique.lookup
   mapping.Semantique.translate

   datacube.Datacube.layout
   datacube.Datacube.lookup
   datacube.Datacube.retrieve

   datacube.Opendatacube.layout
   datacube.Opendatacube.connection
   datacube.Opendatacube.tz
   datacube.Opendatacube.config
   datacube.Opendatacube.lookup
   datacube.Opendatacube.retrieve

   datacube.GeotiffArchive.layout
   datacube.GeotiffArchive.src
   datacube.GeotiffArchive.tz
   datacube.GeotiffArchive.config
   datacube.GeotiffArchive.lookup
   datacube.GeotiffArchive.retrieve

   extent.SpatialExtent.features
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

   components.time
   components.space

   processor.core.QueryProcessor.response
   processor.core.QueryProcessor.recipe
   processor.core.QueryProcessor.datacube
   processor.core.QueryProcessor.mapping
   processor.core.QueryProcessor.extent
   processor.core.QueryProcessor.crs
   processor.core.QueryProcessor.spatial_resolution
   processor.core.QueryProcessor.tz
   processor.core.QueryProcessor.custom_verbs
   processor.core.QueryProcessor.custom_operators
   processor.core.QueryProcessor.custom_reducers
   processor.core.QueryProcessor.track_types
   processor.core.QueryProcessor.parse
   processor.core.QueryProcessor.optimize
   processor.core.QueryProcessor.execute
   processor.core.QueryProcessor.call_handler
   processor.core.QueryProcessor.handle_concept
   processor.core.QueryProcessor.handle_layer
   processor.core.QueryProcessor.handle_result
   processor.core.QueryProcessor.handle_self
   processor.core.QueryProcessor.handle_collection
   processor.core.QueryProcessor.handle_processing_chain
   processor.core.QueryProcessor.handle_verb
   processor.core.QueryProcessor.handle_evaluate
   processor.core.QueryProcessor.handle_extract
   processor.core.QueryProcessor.handle_filter
   processor.core.QueryProcessor.handle_assign
   processor.core.QueryProcessor.handle_groupby
   processor.core.QueryProcessor.handle_reduce
   processor.core.QueryProcessor.handle_shift
   processor.core.QueryProcessor.handle_smooth
   processor.core.QueryProcessor.handle_trim
   processor.core.QueryProcessor.handle_delineate
   processor.core.QueryProcessor.handle_fill
   processor.core.QueryProcessor.handle_name
   processor.core.QueryProcessor.handle_apply_custom
   processor.core.QueryProcessor.handle_compose
   processor.core.QueryProcessor.handle_concatenate
   processor.core.QueryProcessor.handle_merge
   processor.core.QueryProcessor.handle_label
   processor.core.QueryProcessor.handle_set
   processor.core.QueryProcessor.handle_interval
   processor.core.QueryProcessor.handle_geometry
   processor.core.QueryProcessor.handle_time_instant
   processor.core.QueryProcessor.handle_time_interval
   processor.core.QueryProcessor.add_custom_verb
   processor.core.QueryProcessor.call_verb
   processor.core.QueryProcessor.add_custom_operator
   processor.core.QueryProcessor.get_operator
   processor.core.QueryProcessor.add_custom_reducer
   processor.core.QueryProcessor.get_reducer

   processor.arrays.Array.value_type
   processor.arrays.Array.value_labels
   processor.arrays.Array.crs
   processor.arrays.Array.spatial_resolution
   processor.arrays.Array.tz
   processor.arrays.Array.is_empty
   processor.arrays.Array.grid_points
   processor.arrays.Array.evaluate
   processor.arrays.Array.extract
   processor.arrays.Array.filter
   processor.arrays.Array.assign
   processor.arrays.Array.groupby
   processor.arrays.Array.reduce
   processor.arrays.Array.shift
   processor.arrays.Array.smooth
   processor.arrays.Array.trim
   processor.arrays.Array.delineate
   processor.arrays.Array.fill
   processor.arrays.Array.name
   processor.arrays.Array.apply_custom
   processor.arrays.Array.align_with
   processor.arrays.Array.regularize
   processor.arrays.Array.reproject
   processor.arrays.Array.tz_convert
   processor.arrays.Array.write_crs
   processor.arrays.Array.write_tz
   processor.arrays.Array.stack_spatial_dims
   processor.arrays.Array.unstack_spatial_dims
   processor.arrays.Array.stack_all_dims
   processor.arrays.Array.unstack_all_dims
   processor.arrays.Array.drop_non_dimension_coords
   processor.arrays.Array.to_dataframe
   processor.arrays.Array.to_geodataframe
   processor.arrays.Array.to_csv
   processor.arrays.Array.to_geotiff

   processor.arrays.Collection.sq
   processor.arrays.Collection.is_empty
   processor.arrays.Collection.compose
   processor.arrays.Collection.concatenate
   processor.arrays.Collection.merge
   processor.arrays.Collection.evaluate
   processor.arrays.Collection.extract
   processor.arrays.Collection.filter
   processor.arrays.Collection.assign
   processor.arrays.Collection.reduce
   processor.arrays.Collection.shift
   processor.arrays.Collection.smooth
   processor.arrays.Collection.trim
   processor.arrays.Collection.delineate
   processor.arrays.Collection.fill
   processor.arrays.Collection.name
   processor.arrays.Collection.apply_custom
   processor.arrays.Collection.regularize
   processor.arrays.Collection.stack_spatial_dims
   processor.arrays.Collection.unstack_spatial_dims

   processor.values.Interval.sq
   processor.values.Interval.lower
   processor.values.Interval.upper
   processor.values.Interval.value_type

   processor.types.TypePromoter.manual
   processor.types.TypePromoter.input_types
   processor.types.TypePromoter.output_type
   processor.types.TypePromoter.input_labels
   processor.types.TypePromoter.output_labels
   processor.types.TypePromoter.check
   processor.types.TypePromoter.promote