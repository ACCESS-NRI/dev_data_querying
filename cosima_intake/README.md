# Indexing COSIMA using intake

Exporing using intake with COSIMA datasets as a test case.

Currently comprises folders:

- `intake_benchmark`: for a small subset of the COSIMA data, compares the existing cosima-cookbook database approach to a simple intake-esm catalogue that directly indexing the netcdf files.
- `intake_dbdb`: exploring an intake database of intake databases
- `kerchunk_025deg_jra55_iaf_omip2`: kerchunking the COSIMA ACCESS-OM2-025 `025deg_jra55_iaf_omip2` experiment
- `kerchunk_benchmark`: various tests of kerchunk performance/functionality

