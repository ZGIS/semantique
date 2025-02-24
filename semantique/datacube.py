import numpy as np
import xarray as xr

import copy
import datacube
import datetime
import os
import planetary_computer as pc
import pyproj
import pytz
import pystac
import pystac_client
import rasterio
import rioxarray
import stackstac
import warnings

from abc import abstractmethod
from datacube.utils import masking
from pystac_client.stac_api_io import StacApiIO
from rasterio.errors import RasterioIOError
from shapely.geometry import box, shape
from shapely.ops import transform
from urllib3 import Retry

from semantique import exceptions
from semantique.dimensions import TIME, SPACE, X, Y

class Datacube():
  """Base class for EO data cube configurations.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout describing the EO data cube. If :obj:`None`, an empty
      EO data cube instance is constructed.

  """

  def __init__(self, layout = None):
    self.layout = layout

  @property
  def layout(self):
    """:obj:`dict`: The layout file of the EO data cube."""
    return self._layout

  @layout.setter
  def layout(self, value):
    self._layout = {} if value is None else value

  def lookup(self, *reference):
    """Lookup the metadata of a referenced data layer.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.

    Raises
    -------
      :obj:`exceptions.UnknownLayerError`
        If the referenced data layer does not have a metadata object in the
        layout of the EO data cube.

    """
    obj = self._layout
    for key in reference:
      try:
        obj = obj[key]
      except KeyError:
        raise exceptions.UnknownLayerError(
          f"The EO data cube does not contain layer '{reference}'"
        )
    return obj

  @abstractmethod
  def retrieve(self, *reference, extent):
    """Abstract method for the retriever function.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension and two spatial dimensions,
        such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    """
    pass

class Opendatacube(Datacube):
  """Opendatacube specific EO data cube configuration.

  This class is an interface to Opendatacube backends. See the
  `Opendatacube manual`_ for details.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout file describing the EO data cube. If :obj:`None`, an empty
      EO data cube is constructed.
    connection : :obj:`datacube.Datacube`
      Opendatacube interface object allowing to read from the data cube.
    tz
      Timezone of the temporal coordinates in the EO data cube. Can be given as
      :obj:`str` referring to the name of a timezone in the tz database, or
      as instance of any class inheriting from :class:`datetime.tzinfo`.
    **config:
      Additional keyword arguments tuning the data retrieval configuration.
      Valid options are:

      * **trim** (:obj:`bool`): Should each retrieved data layer be trimmed?
        Trimming means that dimension coordinates for which all values are
        missing are removed from the array. The spatial dimensions are trimmed
        only at the edges, to maintain their regularity. Defaults to
        :obj:`True`.

      * **group_by_solar_day** (:obj:`bool`): Should the time dimension be
        resampled to the day level, using solar day to keep scenes together?
        Defaults to :obj:`True`.

      * **value_type_mapping** (:obj:`dict`): How do value type encodings in
        the layout map to the value types used by semantique?
        Defaults to a one-to-one mapping: ::

          {
            "nominal": "nominal",
            "ordinal": "ordinal",
            "binary": "binary",
            "continuous": "continuous",
            "discrete": "discrete"
          }

      * **resamplers** (:obj:`dict`): When data need to be resampled to a
        different spatial and/or temporal resolution, what resampling technique
        should be used? Should be specified separately for each possible value
        type in the layout. Valid techniques are: ::

          'nearest', 'average', 'bilinear', 'cubic', 'cubic_spline',
          'lanczos', 'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'

        Defaults to: ::

          {
            "nominal": "nearest",
            "ordinal": "nearest",
            "binary": "nearest",
            "continuous": "nearest",
            "discrete": "nearest"
          }

  .. _Opendatacube manual:
    https://datacube-core.readthedocs.io/en/latest/index.html

  """

  def __init__(self, layout = None, connection = None, tz = "UTC", **config):
    super(Opendatacube, self).__init__(layout)
    self.connection = connection
    self.tz = tz
    # Update default configuration parameters with provided ones.
    params = self._default_config
    params.update(config)
    self.config = params

  @property
  def connection(self):
    """:obj:`datacube.Datacube`: Opendatacube interface object allowing to read
    from the data cube."""
    return self._connection

  @connection.setter
  def connection(self, value):
    if value is not None:
      assert isinstance(value, datacube.Datacube)
    self._connection = value

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Timezone of the temporal coordinates in the
    EO data cube."""
    return self._tz

  @tz.setter
  def tz(self, value):
    if isinstance(value, datetime.tzinfo):
      self._tz = value
    elif isinstance(value, str):
      self._tz = pytz.timezone(value)
    else:
      raise exceptions.UnknownTimeZoneError(
        f"Cannot recognize {value} as a valid timezone specification"
      )

  @property
  def _default_config(self):
    return {
      "trim": True,
      "group_by_solar_day": True,
      "value_type_mapping": {
        "nominal": "nominal",
        "ordinal": "ordinal",
        "binary": "binary",
        "continuous": "continuous",
        "discrete": "discrete"
      },
      "resamplers": {
        "nominal": "nearest",
        "ordinal": "nearest",
        "binary": "nearest",
        "continuous": "nearest",
        "discrete": "nearest"
      }
    }

  @property
  def config(self):
    """:obj:`dict`: Configuration settings for data retrieval."""
    return self._config

  @config.setter
  def config(self, value):
    assert isinstance(value, dict)
    self._config = value

  @property
  def layout(self):
    """:obj:`dict`: The layout file of the EO data cube."""
    return self._layout

  @layout.setter
  def layout(self, value):
    self._layout = {} if value is None else self._parse_layout(value)

  def _parse_layout(self, obj):
    # Recursively parse metadata objects to make them autocomplete friendly.
    def _parse(current_obj, ref_path):
      if "type" in current_obj and "values" in current_obj:
        current_obj["reference"] = copy.deepcopy(ref_path)
        if isinstance(current_obj["values"], list):
            current_obj["labels"] = {
                item["label"]: item["id"] for item in current_obj["values"]
            }
            current_obj["descriptions"] = {
                item["description"]: item["id"]
                for item in current_obj["values"]
            }
        return
      # If not a "layer", traverse deeper into the object.
      for key, value in current_obj.items():
        if isinstance(value, dict):
          new_ref_path = ref_path + [key]
          _parse(value, new_ref_path)
    # Start parsing from the root object.
    for key, value in obj.items():
      if isinstance(value, dict):
        _parse(value, [key])
    return obj

  def retrieve(self, *reference, extent):
    """Retrieve a data layer from the EO data cube.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension and two spatial dimensions,
        such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    Returns
    -------
      :obj:`xarray.DataArray`
        The retrieved subset of the EO data cube.

    """
    # Solve the reference by obtaining the corresponding metadata object.
    metadata = self.lookup(*reference)
    # Load the data values from the EO data cube.
    data = self._load(metadata, extent)
    if data.sq.is_empty:
      raise exceptions.EmptyDataError(
        f"Data layer '{reference}' does not contain data within the "
        "specified spatio-temporal extent"
      )
    # Format loaded data.
    data = self._format(data, metadata, extent)
    # Mask invalid data.
    data = self._mask(data)
    if data.sq.is_empty:
      warnings.warn(
        f"All values for data layer '{reference}' are invalid within the "
        "specified spatio-temporal extent"
      )
    # Trim the array if requested.
    # This will remove dimension coordinates with only missing or invalid data.
    if self.config["trim"]:
      data = data.sq.trim()
    # PROVISIONAL FIX: Convert value type to float.
    # Sentinel-2 data may be loaded as unsigned integers.
    # This gives problems e.g. with divisions that return negative values.
    # See https://github.com/whisperingpixel/iq-factbase/issues/19.
    # TODO: Find a better way to handle this with less memory footprint.
    data = data.astype("float")
    return data

  def _load(self, metadata, extent):
    # Check if extent is valid.
    if TIME not in extent.dims:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a temporal dimension"
      )
    if X not in extent.dims or Y not in extent.dims:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without spatial dimensions"
      )
    # Create a template for the data to be loaded.
    # This is the given extent modified to be correctly understood by ODC:
    # --> Time and space dimensions should have specific names.
    # --> Time dimension values should be in the data cubes native timezone.
    # --> Class should be `xarray.Dataset` instead of `xarray.DataArray`.
    # Note that the CRS does not have to be in the data cubes native CRS.
    # ODC takes care of spatial transformations internally.
    names = {Y: "y", X: "x", TIME: "time"}
    like = extent.sq.tz_convert(self.tz).sq.rename_dims(names).to_dataset()
    # Call ODC load function to load data as xarray dataset.
    data = self.connection.load(
      product = metadata["product"],
      measurements = [metadata["name"]],
      like = like,
      resampling = self.config["resamplers"][metadata["type"]],
      group_by = "solar_day" if self.config["group_by_solar_day"] else None
    )
    # Return as xarray dataarray.
    try:
      data = data[metadata["name"]]
    except KeyError:
      data = xr.DataArray()
    return data

  def _format(self, data, metadata, extent):
    # Step I: Format temporal coordinates.
    # --> Make sure time dimension has the correct name.
    # --> Convert time coordinates back into the original timezone.
    data = data.sq.rename_dims({"time": TIME})
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    # Step II: Format spatial coordinates.
    # --> Make sure X and Y dims have the correct names.
    # --> Store resolution as an attribute of the spatial coordinate dimensions.
    # --> Add spatial feature indices as a non-dimension coordinate.
    data = data.sq.rename_dims({data.rio.y_dim: Y, data.rio.x_dim: X})
    data = data.sq.write_spatial_resolution(extent.sq.spatial_resolution)
    data.coords["spatial_feats"] = ([Y, X], extent["spatial_feats"].data)
    # Step III: Write semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    data.sq.value_type = self.config["value_type_mapping"][metadata["type"]]
    if isinstance(metadata["values"], list):
      value_labels = {}
      for x in metadata["values"]:
        try:
          label = x["label"]
        except KeyError:
          label = None
        value_labels[x["id"]] = label
      data.sq.value_labels = value_labels
    data[TIME].sq.value_type = "datetime"
    data[Y].sq.value_type = "continuous"
    data[X].sq.value_type = "continuous"
    data["spatial_feats"].sq.value_type = extent["spatial_feats"].sq.value_type
    data["spatial_feats"].sq.value_labels = extent["spatial_feats"].sq.value_labels
    return data

  def _mask(self, data):
    # Step I: Mask nodata values.
    data = masking.mask_invalid_data(data)
    # Step II: Mask values outside of the spatial extent.
    # This is needed since data are initially loaded for the bbox of the extent.
    data = data.where(data["spatial_feats"].notnull())
    return data

class GeotiffArchive(Datacube):
  """EO data cube configuration for zipped GeoTIFF files.

  This simple EO data cube configuration assumes each data layer is a GeoTIFF
  file, and that all layers together are bundled in a ZIP archive. It is meant
  for demonstration purposes.

  Parameters
  ----------
    layout : :obj:`dict`
      The layout file describing the EO data cube. If :obj:`None`, an empty
      EO data cube is constructed.
    src : :obj:`str`
      Path to the ZIP archive containing the data layers.
    tz
      Timezone of the temporal coordinates in the EO data cube. Can be given as
      :obj:`str` referring to the name of a timezone in the tz database, or
      as instance of any class inheriting from :class:`datetime.tzinfo`.
    **config:
      Additional keyword arguments tuning the data retrieval configuration.
      Valid options are:

      * **trim** (:obj:`bool`): Should each retrieved data layer be trimmed?
        Trimming means that dimension coordinates for which all values are
        missing are removed from the array. The spatial dimensions are trimmed
        only at the edges, to maintain their regularity. Defaults to
        :obj:`True`.

      * **value_type_mapping** (:obj:`dict`): How do value type encodings in
        the layout map to the value types used by semantique?
        Defaults to a one-to-one mapping: ::

          {
            "nominal": "nominal",
            "ordinal": "ordinal",
            "binary": "binary",
            "continuous": "continuous",
            "discrete": "discrete"
          }

      * **resamplers** (:obj:`dict`): When data need to be resampled to a
        different spatial and/or temporal resolution, what resampling technique
        should be used? Should be specified separately for each possible value
        type in the layout. Valid techniques are: ::

          'nearest', 'average', 'bilinear', 'cubic', 'cubic_spline',
          'lanczos', 'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'

        Defaults to: ::

          {
            "nominal": "nearest",
            "ordinal": "nearest",
            "binary": "nearest",
            "continuous": "nearest",
            "discrete": "nearest"
          }

  """

  def __init__(self, layout = None, src = None, tz = "UTC", **config):
    super(GeotiffArchive, self).__init__(layout)
    self.src = src
    self.tz = tz
    # Update default configuration parameters with provided ones.
    params = self._default_config
    params.update(config)
    self.config = params

  @property
  def src(self):
    """:obj:`str`: Path to the ZIP archive containing the data layers."""
    return self._src

  @src.setter
  def src(self, value):
    if value is not None:
      assert os.path.splitext(value)[1] == ".zip"
    self._src = value

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Timezone of the temporal coordinates in the
    EO data cube."""
    return self._tz

  @tz.setter
  def tz(self, value):
    if isinstance(value, datetime.tzinfo):
      self._tz = value
    elif isinstance(value, str):
      self._tz = pytz.timezone(value)
    else:
      raise exceptions.UnknownTimeZoneError(
        f"Cannot recognize {value} as a valid timezone specification"
      )

  @property
  def _default_config(self):
    return {
      "trim": True,
      "value_type_mapping": {
        "nominal": "nominal",
        "ordinal": "ordinal",
        "binary": "binary",
        "continuous": "continuous",
        "discrete": "discrete"
      },
      "resamplers": {
        "nominal": "nearest",
        "ordinal": "nearest",
        "binary": "nearest",
        "continuous": "nearest",
        "discrete": "nearest"
      }
    }

  @property
  def config(self):
    """:obj:`dict`: Configuration settings for data retrieval."""
    return self._config

  @config.setter
  def config(self, value):
    assert isinstance(value, dict)
    self._config = value

  @property
  def layout(self):
    """:obj:`dict`: The layout file of the EO data cube."""
    return self._layout

  @layout.setter
  def layout(self, value):
    self._layout = {} if value is None else self._parse_layout(value)

  def _parse_layout(self, obj):
    # Recursively parse metadata objects to make them autocomplete friendly.
    def _parse(current_obj, ref_path):
      if "type" in current_obj and "values" in current_obj:
        current_obj["reference"] = copy.deepcopy(ref_path)
        if isinstance(current_obj["values"], list):
            current_obj["labels"] = {
                item["label"]: item["id"] for item in current_obj["values"]
            }
            current_obj["descriptions"] = {
                item["description"]: item["id"]
                for item in current_obj["values"]
            }
        return
      # If not a "layer", traverse deeper into the object.
      for key, value in current_obj.items():
        if isinstance(value, dict):
          new_ref_path = ref_path + [key]
          _parse(value, new_ref_path)
    # Start parsing from the root object.
    for key, value in obj.items():
      if isinstance(value, dict):
        _parse(value, [key])
    return obj

  def retrieve(self, *reference, extent):
    """Retrieve a data layer from the EO data cube.

    Parameters
    ----------
      *reference:
        The index of the data layer in the layout of the EO data cube.
      extent : :obj:`xarray.DataArray`
        Spatio-temporal extent in which the data should be retrieved. Should be
        given as an array with a temporal dimension and two spatial dimensions,
        such as returned by
        :func:`parse_extent <semantique.processor.utils.parse_extent>`.
        The retrieved subset of the EO data cube will have the same extent.

    Returns
    -------
      :obj:`xarray.DataArray`
        The retrieved subset of the EO data cube.

    """
    # Solve the reference by obtaining the corresponding metadata object.
    metadata = self.lookup(*reference)
    # Load data.
    # This loads all data in the layer into memory (NOT EFFICIENT!).
    # Only after that is subsets the loaded data in space and time.
    data = self._load(metadata, extent)
    if data.sq.is_empty:
      raise exceptions.EmptyDataError(
        f"Data layer '{reference}' does not contain data within the "
        "specified spatio-temporal extent"
      )
    # Format loaded data.
    data = self._format(data, metadata, extent)
    # Mask invalid data.
    data = self._mask(data)
    if data.sq.is_empty:
      warnings.warn(
        f"All values for data layer '{reference}' are invalid within the "
        "specified spatio-temporal extent"
      )
    # Trim the array if requested.
    # This will remove dimension coordinates with only missing or invalid data.
    if self.config["trim"]:
      data = data.sq.trim()
    # PROVISIONAL FIX: Convert value type to float.
    # Sentinel-2 data may be loaded as unsigned integers.
    # This gives problems e.g. with divisions that return negative values.
    # See https://github.com/whisperingpixel/iq-factbase/issues/19.
    # TODO: Find a better way to handle this with less memory footprint.
    data = data.astype("float")
    return data

  def _load(self, metadata, extent):
    # Check if extent is valid.
    if TIME not in extent.dims:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without a temporal dimension"
      )
    if X not in extent.dims and Y not in extent.dims:
      raise exceptions.MissingDimensionError(
        "Cannot retrieve data in an extent without spatial dimensions"
      )
    # Load the complete layer.
    data = rioxarray.open_rasterio("zip://" + self.src + "!" + metadata["file"])
    # Prepare the extent for subsetting.
    # This means dimension names need to be aligned to the loaded data.
    # And the time values need to be converted into the loaded data timezone.
    names = {Y: data.rio.y_dim, X: data.rio.x_dim, TIME: "band"}
    like = extent.sq.tz_convert(self.tz).sq.rename_dims(names)
    # Subset temporally.
    bounds = like["band"].values
    times = [np.datetime64(x, "ns") for x in data.attrs["long_name"]]
    keep = [x >= bounds[0] and x <= bounds[1] for x in times]
    data = data.sel({"band": keep})
    data = data.assign_coords({"band": [x for x, y in zip(times, keep) if y]})
    # Subset spatially.
    resampler_name = self.config["resamplers"][metadata["type"]]
    resampler_func = getattr(rasterio.enums.Resampling, resampler_name)
    data = data.rio.reproject_match(like, resampling = resampler_func)
    # Return subsetted data.
    return data

  def _format(self, data, metadata, extent):
    # Step I: Format temporal coordinates.
    # --> Make sure time dimension has the correct name.
    # --> Convert time coordinates back into the original timezone.
    # --> Delete the long_name attribute that stored the band names.
    data = data.sq.rename_dims({"band": TIME})
    data = data.sq.write_tz(self.tz)
    data = data.sq.tz_convert(extent.sq.tz)
    del data.attrs["long_name"]
    # Step II: Format spatial coordinates.
    # --> Make sure X and Y dims have the correct names.
    # --> Store resolution as an attribute of the spatial coordinate dimensions.
    # --> Add spatial feature indices as a non-dimension coordinate.
    data = data.sq.rename_dims({data.rio.y_dim: Y, data.rio.x_dim: X})
    data = data.sq.write_spatial_resolution(extent.sq.spatial_resolution)
    data.coords["spatial_feats"] = ([Y, X], extent["spatial_feats"].data)
    # Step III: Write semantique specific attributes.
    # --> Value types for the data and all dimension coordinates.
    # --> Mapping from category labels to indices for all categorical data.
    data.sq.value_type = self.config["value_type_mapping"][metadata["type"]]
    if isinstance(metadata["values"], list):
      value_labels = {}
      for x in metadata["values"]:
        try:
          label = x["label"]
        except KeyError:
          label = None
        value_labels[x["id"]] = label
      data.sq.value_labels = value_labels
    data[TIME].sq.value_type = "datetime"
    data[Y].sq.value_type = "continuous"
    data[X].sq.value_type = "continuous"
    data["spatial_feats"].sq.value_type = extent["spatial_feats"].sq.value_type
    data["spatial_feats"].sq.value_labels = extent["spatial_feats"].sq.value_labels
    # Step V: Give the array a name.
    data.name = os.path.splitext(metadata["file"])[0]
    return data

  def _mask(self, data):
    # Step I: Mask nodata values.
    data = data.where(data != data.rio.nodata)
    # Step II: Mask values outside of the spatial extent.
    # This is needed since data are initially loaded for the bbox of the extent.
    data = data.where(data["spatial_feats"].notnull())
    return data


class STACCube(Datacube):
    """
    EO data cube configuration for results from STAC searches.

    STACCube loads data from an item collection and fetches the data into an xarray.

    Parameters
    ----------
      layout : :obj:`dict`
        The layout file describing the EO data cube. If :obj:`None`, an empty
        EO data cube is constructed.
      src : :obj:`pystac.item_collection.ItemCollection` or `list of pystac.item.Item`
        The item search result from a previous STAC search as a src to build the datacube
      **config:
        Additional keyword arguments tuning the data retrieval configuration.
        Valid options are:

        * **trim** (:obj:`bool`): Should each retrieved data layer be trimmed?
          Trimming means that dimension coordinates for which all values are
          missing are removed from the array. The spatial dimensions are trimmed
          only at the edges, to maintain their regularity. Defaults to
          :obj:`True`.

        * **group_by_solar_day** (:obj:`bool`): Should the time dimension be
          resampled to the day level, using solar day to keep scenes together?
          Defaults to :obj:`True`.

        * **dask_params** (:obj:`dict`): Parameters passed to the .compute() function
        when fetching data via the stackstac API. Can be used to control the parallelism
        in fetching data. Defaults to :obj:`None`, i.e. to use the threaded scheduler as
        set by dask as a default for arrays.

        * **reauth_individual** (:obj:`bool`): Should the items be resigned/reauthenticated
        before loading them? Defaults to False.

        * **access_token** (:obj:`str`): Access token string (OAuth2) to be used in accessing the STAC href.

        * **value_type_mapping** (:obj:`dict`): How do value type encodings in
          the layout map to the value types used by semantique?
          Defaults to a one-to-one mapping: ::

            {
              "nominal": "nominal",
              "ordinal": "ordinal",
              "binary": "binary",
              "continuous": "continuous",
              "discrete": "discrete"
            }

        * **resamplers** (:obj:`dict`): When data need to be resampled to a
          different spatial and/or temporal resolution, what resampling technique
          should be used? Should be specified separately for each possible value
          type in the layout. Valid techniques are: ::

            'nearest', 'average', 'bilinear', 'cubic', 'cubic_spline',
            'lanczos', 'mode', 'gauss',  'max', 'min', 'med', 'q1', 'q3'

          Defaults to: ::

            {
              "nominal": "nearest",
              "ordinal": "nearest",
              "binary": "nearest",
              "continuous": "bilinear",
              "discrete": "nearest"
            }

    """

    def __init__(self, layout=None, src=None, **config):
        super(STACCube, self).__init__(layout)
        self.src = src
        # Timezone of the temporal coordinates is infered from the pystac search results
        # & automatically converted to UTC internally - result is given back as datetime64[ns]
        self.tz = "UTC"
        # Update default configuration parameters with provided ones.
        params = self._default_config
        params.update(config)
        self.config = params

    @property
    def src(self):
        """:obj:`pystac.item_collection.ItemCollection` or :obj:`list of pystac.item.Item`:
        The item search result from a previous STAC search."""
        return self._src

    @src.setter
    def src(self, value):
        if value is not None:
            assert np.all([isinstance(x, pystac.item.Item) for x in value])
        self._src = value
   
    @property
    def _default_config(self):
        return {
            "trim": True,
            "group_by_solar_day": True,
            "dask_params": None,
            "reauth_individual": False,
            "access_token": "",
            "value_type_mapping": {
                "nominal": "nominal",
                "ordinal": "ordinal",
                "binary": "binary",
                "continuous": "continuous",
                "discrete": "discrete",
            },
            "resamplers": {
                "nominal": "nearest",
                "ordinal": "nearest",
                "binary": "nearest",
                "continuous": "bilinear",
                "discrete": "nearest",
            },
        }

    @property
    def config(self):
        """:obj:`dict`: Configuration settings for data retrieval."""
        return self._config

    @config.setter
    def config(self, value):
        assert isinstance(value, dict)
        self._config = value

    @property
    def layout(self):
        """:obj:`dict`: The layout file of the EO data cube."""
        return self._layout

    @layout.setter
    def layout(self, value):
        self._layout = {} if value is None else self._parse_layout(value)

    def _parse_layout(self, obj):
        # Function to recursively parse and metadata objects to make them autocomplete friendly
        def _parse(current_obj, ref_path):
            if "type" in current_obj and "values" in current_obj:
                current_obj["reference"] = copy.deepcopy(ref_path)
                if isinstance(current_obj["values"], list):
                    current_obj["labels"] = {
                        item["label"]: item["id"] for item in current_obj["values"]
                    }
                    current_obj["descriptions"] = {
                        item["description"]: item["id"]
                        for item in current_obj["values"]
                    }
                return

            # If not a "layer", traverse deeper into the object.
            for key, value in current_obj.items():
                if isinstance(value, dict):
                    new_ref_path = ref_path + [key]
                    _parse(value, new_ref_path)

        # Start parsing from the root object.
        for key, value in obj.items():
            if isinstance(value, dict):
                _parse(value, [key])
        return obj

    def retrieve(self, *reference, extent):
        """Retrieve a data layer from the EO data cube.

        Parameters
        ----------
          *reference:
            The index of the data layer in the layout of the EO data cube.
          extent : :obj:`xarray.DataArray`
            Spatio-temporal extent in which the data should be retrieved. Should be
            given as an array with a temporal dimension and two spatial dimensions,
            such as returned by
            :func:`parse_extent <semantique.processor.utils.parse_extent>`.
            The retrieved subset of the EO data cube will have the same extent.

        Returns
        -------
          :obj:`xarray.DataArray`
            The retrieved subset of the EO data cube.

        """
        # Solve the reference by obtaining the corresponding metadata object.
        metadata = self.lookup(*reference)
        # Load the data values from the EO data cube.
        data = self._load(metadata, extent)
        if data.sq.is_empty:
            raise exceptions.EmptyDataError(
                f"Data layer '{reference}' does not contain data within the "
                "specified spatio-temporal extent"
            )
        # Format loaded data.
        data = self._format(data, metadata, extent)
        # Mask invalid data.
        data = self._mask(data, metadata)
        if data.sq.is_empty:
            warnings.warn(
                f"All values for data layer '{reference}' are invalid within the "
                "specified spatio-temporal extent"
            )
        # Trim the array if requested.
        # This will remove dimension coordinates with only missing or invalid data.
        if self.config["trim"]:
            data = data.sq.trim()
        return data

    def _load(self, metadata, extent):
        # check if extent is valid
        if TIME not in extent.dims:
            raise exceptions.MissingDimensionError(
                "Cannot retrieve data in an extent without a temporal dimension"
            )
        if X not in extent.dims and Y not in extent.dims:
            raise exceptions.MissingDimensionError(
                "Cannot retrieve data in an extent without spatial dimensions"
            )

        # retrieve spatial bounds, resolution & epsg
        # note: round to avoid binary format <-> floating-point number inconsistencies
        s_bounds = tuple(np.array(extent.rio.bounds()).round(8))
        res = tuple(np.abs(extent.rio.resolution()).round(8))
        epsg = int(str(extent.rio.crs)[5:])

        # retrieve resampler
        resampler_name = self.config["resamplers"][metadata["type"]]
        resampler_func = getattr(rasterio.enums.Resampling, resampler_name)

        # retrieve layer specific information
        lyr_dtype, lyr_na = self._get_dtype_na(metadata)

        # subset temporally and spatially
        if "spatial_feats" in extent.coords:
            extent = extent.drop_vars("spatial_feats")
        t_bounds = extent.sq.tz_convert(self.tz).time.values
        item_coll = STACCube.filter_spatio_temporal(
          self.src,
          extent.rio.bounds(),
          epsg,
          t_bounds[0],
          t_bounds[1]
        )

        # subset according to layer key
        filtered_items = []
        for item in item_coll:
            has_no_key = True
            has_conformant_key = False
            for asset_key, asset in item.assets.items():
                if 'semantique:key' in asset.extra_fields:
                    has_no_key = False
                    asset_key = asset.extra_fields['semantique:key']
                    ref_key = metadata['reference']
                    if "_".join(asset_key) == "_".join(ref_key):
                        has_conformant_key = True
                        break
                else:
                    continue
            if has_no_key or has_conformant_key:
                filtered_items.append(item)
        item_coll = filtered_items

        # return extent array as NaN in case of no data
        if not len(item_coll):
            empty_arr = xr.full_like(extent, np.nan)
            return empty_arr

        stackstac_inputs = {
            "assets": [metadata["name"]],
            "resampling": resampler_func,
            "bounds": s_bounds,
            "epsg": epsg,
            "resolution": res,
            "fill_value": lyr_na,
            "dtype": lyr_dtype,
            "rescale": False,
            "errors_as_nodata": (RasterioIOError(".*"),),
            "xy_coords": "center",
            "snap_bounds": False,
        }

        # reauth
        if self.config["reauth_individual"]:
            item_coll = STACCube._sign_metadata(item_coll)

        # auth via token
        if self.config["access_token"]:
            gdal_env = stackstac.rio_env.LayeredEnv(
                always=dict(
                    GDAL_HTTP_AUTH="BEARER",
                    GDAL_HTTP_BEARER=self.config["access_token"],
                ), )

            stackstac_inputs["gdal_env"] = gdal_env

        data = stackstac.stack(
            item_coll,
            **stackstac_inputs
        )
        data = data.compute(**(self.config["dask_params"] or {}))

        # mosaicking in case of temporal grouping
        # convert datetimes to daily granularity - resample by day
        def _mosaic_ints(x, axis=0, na_value=np.nan):
            max_idx = np.argmax(x != na_value, axis=axis)
            grid_x, grid_y = np.ogrid[:x.shape[2], :x.shape[3]]
            chosen = x[max_idx, 0, grid_x, grid_y]
            na_array = np.full(chosen.shape, na_value, dtype=x.dtype)
            all_na = np.all(x == na_value, axis=axis)
            return np.where(all_na, na_array, chosen)

        if self.config["group_by_solar_day"]:
            if len(data.time):
                days = data.time.astype("datetime64[ns]").dt.floor("D")
                if data.dtype.kind == "f":
                    data = data.where(data != lyr_na)
                    data = (
                        data
                        .groupby(days, squeeze=False)
                        .first(skipna=True)
                        .rename({"floor": "time"})
                    )
                else:
                    data = (
                        data
                        .groupby(days, squeeze=False)
                        .reduce(_mosaic_ints, na_value=lyr_na)
                        .rename({"floor": "time"})
                    )
                data["time"] = data.time.values

        return data

    def _get_dtype_na(self, metadata):
        # retrieve dtype
        try:
            lyr_dtype = np.dtype(metadata["dtype"])
        except:
            lyr_dtype = "float32"
        # retrieve na_value
        try:
            lyr_na = np.array([metadata["na_value"]], dtype=lyr_dtype)[0]
        except:
            if isinstance(np.array([1], dtype=lyr_dtype)[0], np.floating):
                lyr_na = np.nan
            else:
                lyr_na = 0
        # return both
        return lyr_dtype, lyr_na

    def _format(self, data, metadata, extent):
        # Step I: Set band as array name.
        data.name = str(data["band"][0].values)
        data = data.squeeze(dim="band", drop=True)
        # Step II: Drop unnecessary dimensions & coordinates.
        keep_coords = ["time", data.rio.x_dim, data.rio.y_dim]
        drop_coords = [x for x in list(data.coords) if x not in keep_coords]
        data = data.drop_vars(drop_coords)
        # Step III: Format temporal coordinates.
        # --> Make sure time dimension has the correct name.
        # --> Convert time coordinates back into the original timezone.
        data = data.sq.rename_dims({"time": TIME})
        data = data.sq.write_tz(self.tz)
        data = data.sq.tz_convert(extent.sq.tz)
        # Step IV: Format spatial coordinates.
        # --> Make sure X and Y dims have the correct names.
        # --> Store resolution as an attribute of the spatial coordinate dimensions.
        # --> Add spatial feature indices as a non-dimension coordinate.
        data = data.sq.rename_dims({data.rio.y_dim: Y, data.rio.x_dim: X})
        data = data.sq.write_spatial_resolution(extent.sq.spatial_resolution)
        data.coords["spatial_feats"] = ([Y, X], extent["spatial_feats"].data)
        # Step V: Write semantique specific attributes.
        # --> Value types for the data and all dimension coordinates.
        # --> Mapping from category labels to indices for all categorical data.
        data.sq.value_type = self.config["value_type_mapping"][metadata["type"]]
        if isinstance(metadata["values"], list):
            value_labels = {}
            for x in metadata["values"]:
                try:
                    label = x["label"]
                except KeyError:
                    label = None
                value_labels[x["id"]] = label
            data.sq.value_labels = value_labels
        data[TIME].sq.value_type = "datetime"
        data[Y].sq.value_type = "continuous"
        data[X].sq.value_type = "continuous"
        data["spatial_feats"].sq.value_type = extent["spatial_feats"].sq.value_type
        data["spatial_feats"].sq.value_labels = extent["spatial_feats"].sq.value_labels
        return data

    def _mask(self, data, metadata):
        # Step I: Mask nodata values.
        _, lyr_na = self._get_dtype_na(metadata)
        data = data.where(data != lyr_na)
        data = data.where(data != data.rio.nodata)
        # Step II: Mask values outside of the spatial extent.
        # This is needed since data are initially loaded for the bbox of the extent.
        data = data.where(data["spatial_feats"].notnull())
        return data

    @staticmethod
    def _divide_chunks(lst, k):
        return [lst[i : i + k] for i in range(0, len(lst), k)]

    @staticmethod
    def filter_spatio_temporal(item_collection, bbox, bbox_crs, start_datetime, end_datetime):
        """
        Filter item collection by spatio-temporal extent.

        Args:
          item_collection (pystac.ItemCollection): The item collection to filter.
          bbox (tuple): The bounding box in WGS84 coordinates to filter by.
          bbox_crs (str): The CRS of the bounding box.
          start_datetime (np.datetime64): The start datetime to filter by.
          end_datetime (np.datetime64): The end datetime to filter by.
        """
        min_lon, min_lat, max_lon, max_lat = bbox
        spatial_filter = box(min_lon, min_lat, max_lon, max_lat)
        source_crs = pyproj.CRS("EPSG:4326")
        target_crs = pyproj.CRS(bbox_crs)
        transformer = (
            pyproj.Transformer
            .from_crs(source_crs, target_crs, always_xy=True)
            .transform
        )
        filtered_items = []
        for item in item_collection:
            item_geom = shape(item.geometry)
            item_geom = transform(transformer, item_geom)
            item_datetime = np.datetime64(item.datetime)
            if not spatial_filter.intersects(item_geom):
                continue
            if not (start_datetime <= item_datetime < end_datetime):
                continue
            filtered_items.append(item)
        return filtered_items

    @staticmethod
    def _sign_metadata(items):
        # retrieve collections root & item ids
        roots = [x.get_root_link().href for x in items]
        # create dictionary grouped by collection
        curr_colls = {}
        for c, item in zip(roots, items):
            if c not in curr_colls:
                curr_colls[c] = {"items": []}
            curr_colls[c]["items"].append(item)
        # define collections requiring authentication
        # dict with collection and modifier
        auth_colls = {}
        auth_colls = {
            "https://planetarycomputer.microsoft.com/api/stac/v1": pc.sign_inplace
        }
        # update signature for items
        updated_items = []
        for coll in curr_colls.keys():
            if coll in auth_colls.keys():
                # perform search again to renew authentification
                retry = Retry(
                    total=5,
                    backoff_factor=1,
                    status_forcelist=[408, 502, 503, 504],
                    allowed_methods=None,
                )
                client = pystac_client.Client.open(
                    coll,
                    modifier=auth_colls[coll],
                    stac_io=StacApiIO(max_retries=retry, timeout=1800),
                )
                item_chunks = STACCube._divide_chunks(curr_colls[coll]["items"], 100)
                for chunk in item_chunks:
                    item_search = client.search(
                        ids=[x.id for x in chunk],
                        collections=[x.get_collection() for x in chunk],
                    )
                    for item in item_search.items():
                        original_item = next(
                            (i for i in chunk if i.id == item.id), None
                        )
                        if original_item is not None:
                            # create a deep copy of the original item
                            # aim: keep original attributes and assets
                            new_item = original_item.clone()
                            # imprinting of the updated hrefs with new tokens
                            for asset_key in item.assets:
                                if asset_key in new_item.assets:
                                    new_href = item.assets[asset_key].href
                                    new_item.assets[asset_key].href = new_href
                            updated_items.append(new_item)
            else:
                updated_items.extend(curr_colls[coll]["items"])
        # return signed items
        return pystac.ItemCollection(updated_items)