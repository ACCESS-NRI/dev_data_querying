# Indexing COSIMA using intake

Exporing using intake with COSIMA datasets as a test case.

Currently comprises two notebooks:

- `cosima_intake-esm.ipynb` : for a small subset of the COSIMA data, compares the existing cosima-cookbook database approach to a simple intake-esm catalogue that directly indexing the netcdf files.
- `cosima_kerchunk.ipynb` : for a small subset of the COSIMA data, explores using intake to index virtual kerchunk datasets generated for entire experiments and read using zarr.
