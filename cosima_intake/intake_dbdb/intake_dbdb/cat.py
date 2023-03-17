# Hack intake-esm to create an intake-esm-like catalog of intake-esm catalogs

import typing
import pydantic

import json
import fsspec

import pandas as pd

from intake_esm import esm_datastore
from intake_esm.derived import default_registry
from intake_esm.cat import ESMCatalogModel
from intake.source.base import DataSource, Schema

class MetaDataSourceError(Exception):
    pass

class MetaDataSource(DataSource):
    version = '0.1'
    container = 'catalog'
    name = 'meta_esm_datasource'
    partition_access = None

    @pydantic.validate_arguments
    def __init__(
        self,
        key: pydantic.StrictStr,
        records: list[dict[str, typing.Any]],
        path_column_name: pydantic.StrictStr,
        *,
        intake_kwargs: dict[str, typing.Any] = None,
    ):
        """An intake compatible Data Source for catalogs of ESM data.

        Parameters
        ----------
        key: str
            The key of the data source.
        records: list of dict
            A list of records, each of which is a dictionary
            mapping column names to values.
        path_column_name: str
            The column name of the path.
        intake_kwargs: dict, optional
            Additional keyword arguments are passed through to the
            :py:class:`~intake.source.base.DataSource` base class.
        """

        intake_kwargs = intake_kwargs or {}
        super().__init__(**intake_kwargs)
        self.key = key
        self.path_column_name = path_column_name
        self.df = pd.DataFrame.from_records(records)
        self._catalog = None

    def __repr__(self) -> str:
        return f'<{type(self).__name__}  (name: {self.key})>'

    def _get_schema(self) -> Schema:
        if self._catalog is None:
            self._open_catalog()
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
            )
        return self._schema

    def _open_catalog(self):
        """ Open a catalog """
        self._catalog = intake.open_esm_datastore(
            self.df.iloc[0][self.path_column_name],
            read_csv_kwargs={"converters": {"variable": ast.literal_eval}}
        )

    def to_catalog(self):
        """Return a catalog"""
        self._load_metadata()
        return self._catalog

    def close(self):
        """Delete open catalogs from memory"""
        self._catalog = None
        self._schema = None
            
class MetaCatalogModel(ESMCatalogModel):
    @classmethod
    def load(
        cls,
        json_file: typing.Union[str, pydantic.FilePath, pydantic.AnyUrl],
        storage_options: dict[str, typing.Any] = None,
        read_csv_kwargs: dict[str, typing.Any] = None,
    ) -> 'MetaCatalogModel':
        """
        Loads the catalog from a file

        Parameters
        -----------
        json_file: str or pathlib.Path
            The path to the json file containing the meta-esm catalog
        storage_options: dict
            fsspec parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.
        read_csv_kwargs: dict
            Additional keyword arguments passed through to the :py:func:`~pandas.read_csv` function.

        """
        storage_options = storage_options if storage_options is not None else {}
        read_csv_kwargs = read_csv_kwargs or {}
        json_file = str(json_file)  # We accept Path, but fsspec doesn't.
        _mapper = fsspec.get_mapper(json_file, **storage_options)

        with fsspec.open(json_file, **storage_options) as fobj:
            data = json.loads(fobj.read())
            if 'last_updated' not in data:
                data['last_updated'] = None
            cat = cls.parse_obj(data)
            if cat.catalog_file:
                if _mapper.fs.exists(cat.catalog_file):
                    csv_path = cat.catalog_file
                else:
                    csv_path = f'{os.path.dirname(_mapper.root)}/{cat.catalog_file}'
                cat.catalog_file = csv_path
                df = pd.read_csv(
                    cat.catalog_file,
                    storage_options=storage_options,
                    **read_csv_kwargs,
                )
            else:
                df = pd.DataFrame(cat.catalog_dict)

            cat._df = df
            cat._cast_agg_columns_with_iterables()
            return cat
        
class meta_datastore(esm_datastore):
    def __init__(
        self,
        obj: typing.Union[pydantic.FilePath, pydantic.AnyUrl, dict[str, typing.Any]],
        *,
        progressbar: bool = True,
        sep: str = '.',
        # registry: typing.Optional[DerivedVariableRegistry] = None,
        read_csv_kwargs: dict[str, typing.Any] = None,
        storage_options: dict[str, typing.Any] = None,
        **intake_kwargs: dict[str, typing.Any],
    ):
        """Intake Catalog representing an ESM Collection."""
        # super().__init__(**intake_kwargs)
        self.storage_options = storage_options or {}
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.progressbar = progressbar
        self.sep = sep
        if isinstance(obj, dict):
            self.esmcat = MetaCatalogModel.from_dict(obj)
        else:
            self.esmcat = MetaCatalogModel.load(
                obj, storage_options=self.storage_options, read_csv_kwargs=read_csv_kwargs
            )

        # self.derivedcat = registry or default_registry
        self.derivedcat = default_registry
        self._entries = {}
        self._requested_variables = []
        self.datasets = {}
        self._validate_derivedcat()
        
    @pydantic.validate_arguments
    def __getitem__(self, key: str) -> MetaDataSource:
        """
        This method takes a key argument and return a data source
        corresponding to assets (files) .
        """
        # The canonical unique key is the key of a compatible group of assets
        try:
            return self._entries[key]
        except KeyError as e:
            if key in self.keys():
                keys_dict = self.esmcat._construct_group_keys(sep=self.sep)
                grouped = self.esmcat.grouped

                internal_key = keys_dict[key]

                if isinstance(grouped, pd.DataFrame):
                    records = [grouped.loc[internal_key].to_dict()]

                else:
                    records = grouped.get_group(internal_key).to_dict(orient='records')

                # Create a new entry
                entry = MetaDataSource(
                    key=key,
                    records=records,
                    path_column_name=self.esmcat.assets.column_name,
                    intake_kwargs={'metadata': {}},
                )
                self._entries[key] = entry
                return self._entries[key]
            raise KeyError(
                f'key={key} not found in catalog. You can access the list of valid keys via the .keys() method.'
            ) from e
        
    def to_catalog_dict(self):
        
        if not self.keys():
            warnings.warn(
                'There are no datasets to load catalogs for! Returning an empty dictionary.',
                UserWarning,
                stacklevel=2,
            )
            return {}
        
        return {key: source().to_catalog() for key, source in self.items()}