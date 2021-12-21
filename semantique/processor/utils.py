import numpy as np
import pandas as pd
import xarray as xr

def convert_datetime64(obj, tz_from, tz_to, **kwargs):
  obj_new = pd.Timestamp(obj).tz_localize(tz_from).tz_convert(tz_to, **kwargs)
  return np.datetime64(obj_new.tz_localize(None))

def create_extent_cube(spatial_extent, temporal_extent, spatial_resolution,
                       temporal_resolution = None, crs = None, tz = None):
  # Rasterize spatial extent.
  space = spatial_extent.rasterize(spatial_resolution, crs, stack = True)
  # Add spatial feature indices as coordinates.
  space.coords["feature"] = ("space", space.data)
  space["feature"].name = "feature"
  space["feature"].sq.value_type = space.sq.value_type
  space["feature"].sq.value_labels = space.sq.value_labels
  # Discretize temporal extent.
  time = temporal_extent.discretize(temporal_resolution, tz)
  # Combine rasterized spatial extent with discretized temporal extent.
  extent = space.expand_dims({"time": time})
  # Add temporal reference.
  extent["time"].sq.value_type = "time"
  extent = extent.sq.write_tz(time.sq.tz)
  # Trim the extent cube.
  # This means we drop all X and Y slices for which all values are nan.
  extent = extent.sq.trim()
  return extent