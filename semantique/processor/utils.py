import numpy as np
import pandas as pd
import xarray as xr

import rioxarray

def convert_datetime64(obj, tz_from, tz_to, **kwargs):
  obj_new = pd.Timestamp(obj).tz_localize(tz_from).tz_convert(tz_to, **kwargs)
  return np.datetime64(obj_new.tz_localize(None))

def create_extent_cube(space, time, spatial_resolution, crs = None, tz = None):
  ## Spatial extent ##
  # Rasterize spatial extent.
  if crs is None:
    crs = space.crs
  space = space.rasterize(crs, spatial_resolution)
  # Update spatial reference.
  # CRS information was already automatically included during rasterizing.
  # In addition we add the GeoTransform attribute.
  # See https://gdal.org/tutorials/geotransforms_tut.html
  space = space.rio.write_transform()
  # Trim the rasterized spatial extent.
  # This means we drop all X and Y slices for which all values are nan.
  space = space.sq.trim()
  # Stack the two spatial dimensions into one.
  space = space.sq.stack_spatial_dims()
  space["space"].sq.value_type = "space"
  # Add spatial feature indices as coordinates.
  space.coords["feature"] = ("space", space.data)
  space["feature"].name = "feature"
  space["feature"].sq.value_type = space.sq.value_type
  space["feature"].sq.value_labels = space.sq.value_labels
  ## Temporal extent ##
  # Define bounds of temporal extent.
  if tz is None:
    tz = time.tz.zone
  start = time.start.tz_convert(tz).tz_localize(None)
  end = time.end.tz_convert(tz).tz_localize(None)
  ## Spatio-temporal extent ##
  # Combine spatial and temporal extent.
  extent = space.expand_dims({"time": [start, end]})
  extent["time"].sq.value_type = "time"
  # Add temporal reference (i.e. time zone).
  extent = extent.sq.write_tz(tz)
  return extent