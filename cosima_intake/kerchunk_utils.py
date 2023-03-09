def create_reference(*single_input, filename, parquet=True, cleanup="all"):
    """
    Create a kerchunk reference file in stages for sets of netcdf4 files. When
    multiple single_inputs are passed, will create reference file for each single_input
    amnd then combine these with kerchunk.combine.merge_vars which assumes coordinates
    and global attributes are identical across the references being combined.
    
    Parameters
    ----------
    single_input: tuple
        Size 2 tuple with the first element containing a list of the netcdf files to be
        combined and the second element containing a dictionary with the kwargs to pass
        to kerchunk.combine.MultiZarrToZarr to combine these files
    filename: str
        The name of the reference file
    parquet: bool
        If True, generate parquet reference file in addition to json
    cleanup: str
        How much of the intermediate reference files to clean up on completion:
            "all": clean up all intermediate files
            "single": clean up single-file reference files
            "none": don't do any clean up
    """
    
    import os
    import time
    import shutil
    import dask
    import zarr
    import ujson
    from distributed import Client
    from kerchunk import hdf, combine, df
    
    @dask.delayed
    def _gen_json(ncfile, jsonfile):
        with open(ncfile) as infile:
            h5chunks = hdf.SingleHdf5ToZarr(infile, ncfile)
            with open(jsonfile, 'wb') as f:
                f.write(ujson.dumps(h5chunks.translate()).encode())
    
    if cleanup not in ["all", "single", "none"]:
        raise ValueError("cleanup must be one of 'all', 'single', 'none'")
    
    dirname = os.path.dirname(filename)
    if dirname != "":
        os.makedirs(os.path.dirname(filename), mode=0o775, exist_ok=True)
    filename = filename.rstrip(".json")
    
    intermediate_jsons = []
    for idx, single in enumerate(single_input):
        ncfiles, mzz_kwargs = single
        
        # Write single file jsons
        single_jsons = []
        tasks = []
        for file in ncfiles:
            single_json = os.path.join(dirname, f"{'.'.join(file.split('/'))}.json")
            single_jsons.append(single_json)
            tasks.append(_gen_json(file, single_json))
            
        with Client(threads_per_worker=1) as client:
            print(f"dask dashboard at {client.dashboard_link}")
            _ = dask.compute(*tasks)
                         
        # Write multi-file json
        intermediate = combine.MultiZarrToZarr(
            single_jsons,
            **mzz_kwargs
        ).translate()
        
        intermediate_json = os.path.join(dirname, f"ref{idx}.json")
        with open(intermediate_json, 'wb') as f:
            f.write(ujson.dumps(intermediate).encode())
        intermediate_jsons.append(intermediate_json)

        if (cleanup == "all") or (cleanup == "single"):            
            for json in single_jsons:
                os.remove(json)
                
    # Combine multifile jsons
    # This assumes coordinates and attributes are the same across intermediates
    combined = combine.merge_vars(
        intermediate_jsons,
    )
    
    unconsolidated_json = os.path.join(dirname, f".{os.path.basename(filename)}.json")
    with open(unconsolidated_json, 'wb') as f:
        f.write(ujson.dumps(combined).encode())
    
    m = fsspec.get_mapper(
        'reference://',
        fo=unconsolidated_json,
        remote_protocol="file"
    )
    zarr.convenience.consolidate_metadata(m)
    m.fs.save_json(f"{filename}.json")

    if parquet:
        parquet_file = f"{filename}.parq"
        if os.path.exists(parquet_file) and os.path.isdir(parquet_file):
            shutil.rmtree(parquet_file)
        os.mkdir(parquet_file)
        df.refs_to_dataframe(combined, parquet_file, partition=True)

    if (cleanup == "all"):
        os.remove(unconsolidated_json)
        for json in intermediate_jsons:
            os.remove(json)