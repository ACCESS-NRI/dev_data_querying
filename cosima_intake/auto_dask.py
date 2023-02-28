# Kerchunk includes a convenience function called auto_dask that is currently
# broken (0.1.0). The version of `auto_dask` below includes a fix.
# See https://github.com/fsspec/kerchunk/issues/306

from typing import List

from kerchunk.combine import MultiZarrToZarr

def auto_dask(
    urls: List[str],
    single_driver: str,
    single_kwargs: dict,
    mzz_kwargs: dict,
    n_batches: int,
    remote_protocol=None,
    remote_options=None,
    filename=None,
    output_options=None,
):
    """Batched tree combine using dask.

    If you wish to run on a distributed cluster (recommended), create
    a client before calling this function.

    Parameters
    ----------
    urls: list[str]
        input dataset URLs
    single_driver: class
        class with ``translate()`` method
    single_kwargs: to pass to single-input driver
    mzz_kwargs: passed to ``MultiZarrToZarr`` for each batch
    n_batches: int
        Number of MZZ instances in the first combine stage. Maybe set equal
        to the number of dask workers, or a multple thereof.
    remote_protocol: str | None
    remote_options: dict
        To fsspec for opening the remote files
    filename: str | None
        Ouput filename, if writing
    output_options
        If ``filename`` is not None, open it with these options

    Returns
    -------
    reference set
    """
    import dask

    # make delayed functions
    single_task = dask.delayed(lambda x: single_driver(x, **single_kwargs).translate())
    post = mzz_kwargs.pop("postprocess", None)
    inline = mzz_kwargs.pop("inline_threshold", None)
    # TODO: if single files produce list of reference sets (e.g., grib2)
    batch_task = dask.delayed(
        lambda u, x: MultiZarrToZarr(
            u,
            indicts=x,
            remote_protocol=remote_protocol,
            remote_options=remote_options,
            **mzz_kwargs,
        ).translate()
    )

    # sort out kwargs
    dims = mzz_kwargs.get("concat_dims", [])
    dims += [k for k in mzz_kwargs.get("coo_map", []) if k not in dims]
    kwargs = {"concat_dims": dims}
    if post:
        kwargs["postprocess"] = post
    if inline:
        kwargs["inline_threshold"] = inline
    for field in ["remote_protocol", "remote_options", "coo_dtypes", "identical_dims"]:
        if field in mzz_kwargs:
            kwargs[field] = mzz_kwargs[field]
    final_task = dask.delayed(
        lambda x: MultiZarrToZarr(
            x, remote_options=remote_options, remote_protocol=remote_protocol, **kwargs
        ).translate(filename, output_options)
    )

    # make delayed calls
    tasks = [single_task(u) for u in urls]
    tasks_per_batch = -(-len(tasks) // n_batches)
    tasks2 = []
    for batch in range(n_batches):
        in_tasks = tasks[batch * tasks_per_batch : (batch + 1) * tasks_per_batch]
        u = urls[batch * tasks_per_batch : (batch + 1) * tasks_per_batch]
        if in_tasks:
            # skip if on last iteration and no remaining tasks
            tasks2.append(batch_task(u, in_tasks))
    
    return dask.compute(final_task(tasks2))[0]