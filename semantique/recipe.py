from semantique.processor.core import QueryProcessor

class QueryRecipe(dict):
  """Dict-like container to store result instructions.

  Parameters
  ----------
    results : :obj:`dict` of :obj:`CubeProxy`
      Dictionary containing result names as keys and result instructions as
      values. If `None`, an empty recipe is constructed.

  """
  def __init__(self, results = None):
    obj = {} if results is None else results
    super(QueryRecipe, self).__init__(obj)

  def execute(self, factbase, ontology, space, time, **config):
    """Execute a query recipe.

    This function initializes a :obj:`processor.core.QueryProcessor` instance
    and uses it to process the query. It runs through all distinct phases of
    query processing: parsing, optimization and execution.

    Parameters
    ----------
      factbase : :obj:`factbase.Factbase`
        The factbase instance to process the query against.
      ontology : :obj:`ontology.Ontology`
        The ontology instance to process the query against.
      space : :obj:`extent.SpatialExtent`
        The spatial extent in which the query should be processed.
      time : :obj:`extent.TemporalExtent`
        The temporal extent in which the query should be processed.
      **config:
        Additional configuration parameters forwarded to
        :func:`processor.core.QueryProcessor.parse`.

    Returns
    -------
      :obj:`dict` of :obj:`xarray.DataArray`:
        Dictionary containing result names as keys and processed results as
        values.

    """
    qp = QueryProcessor.parse(self, factbase, ontology, space, time, **config)
    return qp.optimize().execute().respond()