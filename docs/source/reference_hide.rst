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

   QueryRecipe.execute

   ontology.Ontology.lookup
   ontology.Ontology.translate

   ontology.Semantique.lookup
   ontology.Semantique.translate

   factbase.Factbase.layout
   factbase.Factbase.lookup
   factbase.Factbase.retrieve

   factbase.Opendatacube.connection
   factbase.Opendatacube.tz
   factbase.Opendatacube.config
   factbase.Opendatacube.lookup
   factbase.Opendatacube.retrieve

   factbase.GeotiffArchive.src
   factbase.GeotiffArchive.tz
   factbase.GeotiffArchive.config
   factbase.GeotiffArchive.lookup
   factbase.GeotiffArchive.retrieve

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
   processor.core.QueryProcessor.factbase
   processor.core.QueryProcessor.ontology
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
   processor.core.QueryProcessor.get_handler
   processor.core.QueryProcessor.handle_concept
   processor.core.QueryProcessor.handle_resource
   processor.core.QueryProcessor.handle_result
   processor.core.QueryProcessor.handle_self
   processor.core.QueryProcessor.handle_collection
   processor.core.QueryProcessor.handle_processing_chain
   processor.core.QueryProcessor.handle_value_label
   processor.core.QueryProcessor.handle_geometries
   processor.core.QueryProcessor.handle_time_instant
   processor.core.QueryProcessor.handle_time_interval
   processor.core.QueryProcessor.handle_verb
   processor.core.QueryProcessor.update_params_of_evaluate
   processor.core.QueryProcessor.update_params_of_filter
   processor.core.QueryProcessor.update_params_of_groupby
   processor.core.QueryProcessor.update_params_of_reduce
   processor.core.QueryProcessor.update_params_of_replace
   processor.core.QueryProcessor.update_params_of_compose
   processor.core.QueryProcessor.update_params_of_concatenate
   processor.core.QueryProcessor.update_params_of_merge
   processor.core.QueryProcessor.update_list_elements
   processor.core.QueryProcessor.add_operator
   processor.core.QueryProcessor.get_operator
   processor.core.QueryProcessor.store_default_operators
   processor.core.QueryProcessor.add_reducer
   processor.core.QueryProcessor.get_reducer
   processor.core.QueryProcessor.store_default_reducers

   processor.structures.Cube.value_type
   processor.structures.Cube.value_labels
   processor.structures.Cube.crs
   processor.structures.Cube.spatial_resolution
   processor.structures.Cube.tz
   processor.structures.Cube.extent
   processor.structures.Cube.is_empty
   processor.structures.Cube.temporal_dimension
   processor.structures.Cube.spatial_dimension
   processor.structures.Cube.xy_dimensions
   processor.structures.Cube.evaluate
   processor.structures.Cube.extract
   processor.structures.Cube.filter
   processor.structures.Cube.groupby
   processor.structures.Cube.label
   processor.structures.Cube.reduce
   processor.structures.Cube.align_with
   processor.structures.Cube.trim
   processor.structures.Cube.regularize
   processor.structures.Cube.reproject
   processor.structures.Cube.tz_convert
   processor.structures.Cube.write_crs
   processor.structures.Cube.write_tz
   processor.structures.Cube.stack_spatial_dims
   processor.structures.Cube.unstack_spatial_dims
   processor.structures.Cube.drop_non_dimension_coords
   processor.structures.Cube.promote_value_type
   processor.structures.Cube.to_csv
   processor.structures.Cube.to_geotiff

   processor.structures.CubeCollection.extent
   processor.structures.CubeCollection.is_empty
   processor.structures.CubeCollection.compose
   processor.structures.CubeCollection.concatenate
   processor.structures.CubeCollection.merge
   processor.structures.CubeCollection.evaluate
   processor.structures.CubeCollection.extract
   processor.structures.CubeCollection.filter
   processor.structures.CubeCollection.label
   processor.structures.CubeCollection.reduce
   processor.structures.CubeCollection.trim
   processor.structures.CubeCollection.regularize
   processor.structures.CubeCollection.stack_spatial_dims
   processor.structures.CubeCollection.unstack_spatial_dims