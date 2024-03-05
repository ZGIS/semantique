from setuptools import setup, find_packages

# Get the long description from the README file.
with open("README.md", encoding = "utf-8") as file:
  long_description = file.read()

# List dependencies.
dependencies = [
  'datacube>=1.8',
  'geocube>=0.4.1',
  'geopandas>=0.11',
  'numpy>=1.21',
  'pandas>=2.0',
  'planetary-computer',
  'pyproj>=3.0',
  'pytz',
  'pystac',
  'pystac-client',
  'rasterio',
  'rioxarray>=0.14',
  'scipy>=1.11',
  'setuptools',
  'stackstac>=0.5.0',
  'xarray>=0.20'
]

# Setup.
setup(
  name = "semantique",
  version = "0.1.0",
  description = "Semantic querying of Earth Observation data",
  long_description = long_description,
  long_description_content_type = "text/markdown",
  url = "https://github.com/ZGIS/semantique",
  author = "Lucas van der Meer",
  author_email = "lucas.vandermeer@sbg.ac.at",
  packages = find_packages(),
  python_requires = ">=3.9",
  install_requires = dependencies
)