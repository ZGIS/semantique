from semantique.processor.core import QueryProcessor

class QueryRecipe(dict):
  """Dict-like container to store result instructions.

  In semantique, a query recipe is a dict-like container storing instructions
  that can be executed by a query processor in order to infer new knowledge
  from earth observation data. Each distinct piece of knowledge, called a query
  result, has its own set of instructions, which are constructed by chaining
  together several building blocks offered by semantique.

  In these instructions, one can refer directly to real-world semantic concepts
  by their name, without having to be aware how these concepts are actually
  represented by the earth observations data.

  Parameters
  ----------
    results : :obj:`dict` of :obj:`CubeProxy`
      Dictionary containing result names as keys and result instructions as
      values. If :obj:`None`, an empty recipe is constructed.

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
      factbase : Factbase
        The factbase instance to process the query against.
      ontology : Ontology
        The ontology instance to process the query against.
      space : SpatialExtent
        The spatial extent in which the query should be processed.
      time : TemporalExtent
        The temporal extent in which the query should be processed.
      **config:
        Additional configuration parameters forwarded to
        :func:`QueryProcessor.parse <processor.core.QueryProcessor.parse>`.

    Returns
    -------
      :obj:`dict` of :obj:`xarray.DataArray`:
        The response of the query processor as a dictionary containing result
        names as keys and result arrays as values.

    """
    qp = QueryProcessor.parse(self, factbase, ontology, space, time, **config)
    return qp.optimize().execute().respond()