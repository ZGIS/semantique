from semantique.processor.core import QueryProcessor, FakeProcessor
from semantique.visualiser.visualise import show

class QueryRecipe(dict):
  """Dict-like container to store instructions of a query recipe.

  Parameters
  ----------
    results : :obj:`dict` of :obj:`ArrayProxy`
      Dictionary containing result names as keys and result instructions as
      values. If :obj:`None`, an empty recipe is constructed.

  Returns
  -------
    :obj:`dict` of :obj:`ArrayProxy`:
      The query recipe as a dictionary containing result names as keys and
      result instructions as values.

  Examples
  --------
  >>> import semantique as sq
  >>> recipe = sq.QueryRecipe()
  >>> recipe["map"] = sq.entity("water").reduce("time", "count")
  >>> recipe["series"] = sq.entity("water").reduce("space", "count")

  """
  def __init__(self, results = None):
    obj = {} if results is None else results
    super(QueryRecipe, self).__init__(obj)

  def execute(self, datacube, mapping, space, time, run_preview = False,
              cache_data = True, **config):
    """Execute a query recipe.

    This function initializes a :obj:`processor.core.QueryProcessor` instance
    and uses it to process the query. It runs through all distinct phases of
    query processing: parsing, optimization and execution.

    Parameters
    ----------
      datacube : Datacube
        The datacube instance to process the query against.
      mapping : Mapping
        The mapping instance to process the query against.
      space : SpatialExtent
        The spatial extent in which the query should be processed.
      time : TemporalExtent
        The temporal extent in which the query should be processed.
      run_preview : :obj:`bool`
        Should a preview run with reduced spatial resolution be performed?
        A preview run enables to test if the recipe execution succeeds
        and allows to inspect the results.
      cache_data : :obj:`bool`
        Should the query processor cache the data references as provided by the
        mapped concepts? Enabling caching increases the memory footprint while
        reducing the I/O time to retrieve data if the same data layer is
        referenced multiple times.
      **config:
        Additional configuration parameters forwarded to
        :func:`QueryProcessor.parse <processor.core.QueryProcessor.parse>`.

    Returns
    -------
      :obj:`dict` of :obj:`xarray.DataArray`:
        The response of the query processor as a dictionary containing result
        names as keys and result arrays as values.

    Examples
    --------
    >>> import semantique as sq
    >>> import geopandas as pd

    >>> recipe = sq.QueryRecipe()
    >>> recipe["map"] = sq.entity("water").reduce("time", "count")
    >>> recipe["series"] = sq.entity("water").reduce("space", "count")

    >>> dc = sq.datacube.GeotiffArchive("files/layout_gtiff.json", src = "layers_gtiff.zip")
    >>> mapping = sq.mapping.Semantique("files/mapping.json")
    >>> space = sq.SpatialExtent(gpd.read_file("files/footprint.geojson"))
    >>> time = sq.TemporalExtent("2019-01-01", "2020-12-31")
    >>> config = {"crs": 3035, "tz": "UTC", "spatial_resolution": [-1800, 1800]}

    >>> recipe.execute(dc, mapping, space, time, **config)

    """
    if cache_data:
      fp = FakeProcessor.parse(self, datacube, mapping, space, time, **config)
      _ = fp.optimize().execute()
      cache = fp.cache
    else:
      cache = None

    qp = QueryProcessor.parse(
      self,
      datacube,
      mapping,
      space,
      time,
      preview=run_preview,
      cache=cache,
      **config
    )
    return qp.optimize().execute()

  def visualise(self):
    """Visualise the recipe in a web browser.

    This method visualises the recipe in a web browser. 
    The visualisation is based on Blockly, a web-based visual programming
    editor. The recipe is converted into Blockly XML format and served
    to the browser.
    """
    show(self)
