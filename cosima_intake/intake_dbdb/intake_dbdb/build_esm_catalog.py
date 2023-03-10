import yaml
import jsonschema

import intake_esm
from ecgtools import Builder

import parsers

OUTPUT_DIR = "."
esmcat_version = "0.0.1"
# config_schema = {
#         'type': 'object',
#         'properties': {
#             'model': {'type': 'string'},
#             'catalogues': {
#                 'type': 'object',
#                 'properties': {
#                     'catalogue_names': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                 },
#             },
#             'parser': {'type': 'string'},
#             'search': {
#                 'type': 'object',
#                 'properties': {
#                     'depth': {'type': 'integer'},
#                     'exclude_patterns': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                     'include_patterns': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                 },
#             },
#         },
#         'required': ['id','catalogues','parser','search'],
#     }

def build_esm_catalog(config)
    """
    Build intake-esm catalogue(s) base on a provided config file 
    
    Parameters
    ----------
    config: str
        Path to the config yaml file
    """
    
    with open(config) as f:
        config = yaml.safe_load(f)

    # jsonschema.validate(config, config_schema)
    
    model = config.get("model")
    catalogues = config.get("catalogues")
    parser = getattr(parsers, config.get("parser"))
    build_kwargs = config.get("search")
    aggregation_control = config.get("aggregation_control")
    
    ncpu = multiprocessing.cpu_count()
    
    for cat_name, cat_contents in catalogues.items():
        
        builder = Builder(
            cat_contents["root_dirs"],
            **build_kwargs,
            joblib_parallel_kwargs={"n_jobs": ncpu},
        ).build(
            parsing_func=parser
        )
        
        builder.save(
            name=cat_name,
            path_column_name="path",
            variable_column_name="variable",
            data_format=intake_esm.cat.DataFormat,
            aggregations=aggregations,
            esmcat_version=esmcat_version,
            description=cat_contents["description"],
            directory=OUTPUT_DIR,
            catalog_type='file',
        )
            
