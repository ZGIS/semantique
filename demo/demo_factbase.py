import numpy as np
import os
import pytz
import rioxarray

from semantique.factbase import Factbase
from semantique import exceptions

class Demo(Factbase):
    
  def __init__(self, layout = None, src = None, tz = "UTC"):
    super(Demo, self).__init__(layout)
    self.src = src
    self.tz = tz
    
  @property
  def src(self):
    return self._src
    
  @src.setter
  def src(self, value):
    if value is not None:
      assert os.path.splitext(value)[1] == ".zip"
    self._src = value

  @property
  def tz(self):
    return self._tz

  @tz.setter
  def tz(self, value):
    self._tz = pytz.timezone(value)
    
  def retrieve(self, *reference, extent):
    # Get metadata.
    metadata = self.lookup(*reference)
    # Load the requested data as xarray data array.
    data = rioxarray.open_rasterio("zip://" + self._src + "!" + metadata["file"])
    # Subset temporally.
    bounds = extent.sq.tz_convert(self.tz)[extent.sq.temporal_dimension].values
    times = [np.datetime64(x) for x in metadata["times"]]
    in_bounds = [x >= bounds[0] and x <= bounds[1] for x in times]
    data = data.sel({"band": in_bounds})
    data = data.rename({"band": "time"})
    data = data.assign_coords({"time": [x for x, y in zip(times, in_bounds) if y]})
    # Subset spatially.
    if extent.sq.spatial_dimension is None:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a spatial dimension"
      )
    data = data.rio.reproject_match(extent.sq.unstack_spatial_dims())
    # Convert to correct timezone.
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    # Stack spatial dimensions back together into a single 'space' dimension.
    data = data.sq.stack_spatial_dims()
    # Add spatial feature indices as coordinates.
    data.coords["feature"] = ("space", extent["feature"].data)
    # Add semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    if metadata["type"] == "continuous":
     data.sq.value_type = "numerical"
    else:
     data.sq.value_type = metadata["type"]
    if metadata["type"] == "categorical":
      categories = {}
      for x in metadata["values"]:
        categories[x["label"]] = x["id"]
      data.sq.categories = categories
    data["space"].sq.value_type = "space"
    data["time"].sq.value_type = "time"
    data["feature"].sq.value_type = extent["feature"].sq.value_type
    data["feature"].sq.categories = extent["feature"].sq.categories
    # Add name.
    data.name = os.path.splitext(metadata["file"])[0]
    return data