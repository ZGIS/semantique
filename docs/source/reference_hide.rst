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

   processor.core.QueryProcessor.response
   processor.core.QueryProcessor.recipe
   processor.core.QueryProcessor.datacube
   processor.core.QueryProcessor.mapping
   processor.core.QueryProcessor.extent
   processor.core.QueryProcessor.crs
   processor.core.QueryProcessor.spatial_resolution
   processor.core.QueryProcessor.tz
   processor.core.QueryProcessor.operators
   processor.core.QueryProcessor.reducers
   processor.core.QueryProcessor.track_types
   processor.core.QueryProcessor.trim_filter
   processor.core.QueryProcessor.trim_results
   processor.core.QueryProcessor.unstack_results
   processor.core.QueryProcessor.parse
   processor.core.QueryProcessor.optimize
   processor.core.QueryProcessor.execute
   processor.core.QueryProcessor.respond
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
   processor.core.QueryProcessor.handle_name
   processor.core.QueryProcessor.handle_compose
   processor.core.QueryProcessor.handle_concatenate
   processor.core.QueryProcessor.handle_merge
   processor.core.QueryProcessor.handle_label
   processor.core.QueryProcessor.handle_set
   processor.core.QueryProcessor.handle_interval
   processor.core.QueryProcessor.handle_geometry
   processor.core.QueryProcessor.handle_time_instant
   processor.core.QueryProcessor.handle_time_interval
   processor.core.QueryProcessor.call_verb
   processor.core.QueryProcessor.add_operator
   processor.core.QueryProcessor.get_operator
   processor.core.QueryProcessor.store_default_operators
   processor.core.QueryProcessor.add_reducer
   processor.core.QueryProcessor.get_reducer
   processor.core.QueryProcessor.store_default_reducers

   processor.arrays.SemanticArray.value_type
   processor.arrays.SemanticArray.value_labels
   processor.arrays.SemanticArray.crs
   processor.arrays.SemanticArray.spatial_resolution
   processor.arrays.SemanticArray.tz
   processor.arrays.SemanticArray.is_empty
   processor.arrays.SemanticArray.temporal_dimension
   processor.arrays.SemanticArray.spatial_dimension
   processor.arrays.SemanticArray.grid_points
   processor.arrays.SemanticArray.evaluate
   processor.arrays.SemanticArray.extract
   processor.arrays.SemanticArray.filter
   processor.arrays.SemanticArray.assign
   processor.arrays.SemanticArray.groupby
   processor.arrays.SemanticArray.reduce
   processor.arrays.SemanticArray.shift
   processor.arrays.SemanticArray.smooth
   processor.arrays.SemanticArray.name
   processor.arrays.SemanticArray.align_with
   processor.arrays.SemanticArray.trim
   processor.arrays.SemanticArray.regularize
   processor.arrays.SemanticArray.reproject
   processor.arrays.SemanticArray.tz_convert
   processor.arrays.SemanticArray.write_crs
   processor.arrays.SemanticArray.write_tz
   processor.arrays.SemanticArray.find_spatial_dims
   processor.arrays.SemanticArray.stack_spatial_dims
   processor.arrays.SemanticArray.unstack_spatial_dims
   processor.arrays.SemanticArray.drop_non_dimension_coords
   processor.arrays.SemanticArray.to_dataframe
   processor.arrays.SemanticArray.to_geodataframe
   processor.arrays.SemanticArray.to_csv
   processor.arrays.SemanticArray.to_geotiff

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
   processor.arrays.Collection.name
   processor.arrays.Collection.trim
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