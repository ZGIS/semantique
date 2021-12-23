import numpy as np
import pandas as pd
import xarray as xr

def convert_datetime64(obj, tz_from, tz_to, **kwargs):
  obj_new = pd.Timestamp(obj).tz_localize(tz_from).tz_convert(tz_to, **kwargs)
  return np.datetime64(obj_new.tz_localize(None))

def create_extent_cube(spatial_extent, temporal_extent, spatial_resolution,
                       temporal_resolution = None, crs = None, tz = None,
                       trim = False):
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
  # Trim the extent cube if requested.
  # This means we drop all x, y and time slices for which all values are nan.
  if trim:
    extent = extent.sq.trim()
  return extent

def parse_datetime_component(name, obj):
  if name in ["dayofweek", "weekday"]:
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      "Monday": 0,
      "Tuesday": 1,
      "Wednesday": 2,
      "Thursday": 3,
      "Friday": 4,
      "Saturday": 5,
      "Sunday": 6
    }
  elif name == "quarter":
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      "First": 1,
      "Second": 2,
      "Third": 3,
      "Fourth": 4
    }
  elif name == "season":
    # In xarray seasons get stored as strings.
    # We want to store them as integers instead.
    for k, v in zip(["MAM", "JJA", "SON", "DJF"], [1, 2, 3, 4]):
      obj = obj.str.replace(k, str(v))
    obj = obj.astype(int)
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      "Spring": 1,
      "Summer": 2,
      "Autumn": 3,
      "Winter": 4
    }
  else:
    obj.sq.value_type = "numerical"
  return obj