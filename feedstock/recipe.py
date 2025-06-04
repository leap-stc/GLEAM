import xarray as xr
import os
import re
import logging
import gcsfs
import gc
from dask.distributed import Client, LocalCluster
# ------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------
logging.basicConfig(
    filename='prepare_gleam_zarr.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s'
)
logging.info("Starting recipe.py")

def get_chunk_scheme(ds, target_MB=100):
    """
    Estimate chunking so that each chunk is ≲ target_MB MB.
    Returns a dict of {'time': ..., 'lat': ..., 'lon': ...}.
    """
    var = next(iter(ds.data_vars))
    nbytes = ds[var].dtype.itemsize
    lat, lon, time = ds.sizes['lat'], ds.sizes['lon'], ds.sizes['time']
    full_MB = (lat * lon * nbytes) / (1024**2)
    if full_MB <= target_MB:
        return {'time': time}
    # allocate spatial chunks to fit target_MB for full time slice
    allowed_points = (target_MB * 1024**2) / nbytes
    ratio = lat / lon
    lat_chunk = max(1, min(lat, int((allowed_points * ratio)**0.5)))
    lon_chunk = max(1, min(lon, int((allowed_points / ratio)**0.5)))
    return {'time': time, 'lat': lat_chunk, 'lon': lon_chunk}

def main():
    # ------------------------------------------------------------------
    # Settings
    # ------------------------------------------------------------------
    bucket_prefix = "leap-scratch/mitraa90/GLEAM"
    variables = ["SMs", "Ei", "E", "H", "Et", "Ew", "Ep_rad", "Ep", "Ec", "SMrz", "Es", "Eb", "Ep_aero", "S"]#["Es"]#,"SMs","Ei","E","H","Et","Ew","Ep_rad","Ep","Ec","SMrz","Eb","Ep_aero","S"]
    #    target_MB = 100  # MB per chunk

    # ------------------------------------------------------------------
    # Start Dask & GCS
    # ------------------------------------------------------------------
    cluster = LocalCluster(n_workers=8, threads_per_worker=1, memory_limit="7GB")
    client = Client(cluster)
    n_workers = len(cluster.workers)
    logging.info(f"Dask cluster started with {n_workers} workers")

    fs = gcsfs.GCSFileSystem()

    # List all .nc files
    all_files = [f for f in fs.ls(bucket_prefix) if f.endswith('.nc')]
    for var in variables:
        # Filter files for this variable
        pattern = re.compile(rf"^{re.escape(var)}_\d{{4}}_")
        var_files = [
            f for f in all_files
            if pattern.match(os.path.basename(f))
        ]

        if not var_files:
            logging.warning(f"No files for variable {var}")
            continue
        print(f"Found {len(var_files)} NetCDF files for variable {var}")
        pattern = re.compile(rf"^{re.escape(var)}_\d{{4}}_")
        var_files = sorted(f for f in all_files if pattern.match(os.path.basename(f)))
        # Batch & inspect
        batch_size = 4
        batches = [var_files[i:i+batch_size] for i in range(0, len(var_files), batch_size)]
        target_store = f"gs://leap-persistent/data-library/GLEAM/GLEAM-{var}.zarr"
        first = True
        for idx, batch in enumerate(batches, start=1):
            print(f"\nVariable {var}: Batch {idx}/{len(batches)} with {len(batch)} files")
            # open_mfdataset in a with‐block, no fsspec cache
            files = [fs.open(p, "rb", cache_type="none") for p in batch]
            # 2) pass them into open_mfdataset, but disable the parallel reader
            with xr.open_mfdataset(
                files,
                engine='h5netcdf',
                chunks={},
                combine='by_coords',
                parallel=False
            ) as ds:
                print(" files are openned")
                chunk_scheme={'time': 487, 'lat': 200, 'lon': 300}
                ds = ds.chunk(chunk_scheme)
                print("data rechunked")
                if idx == 1:
                    dtype_bytes = ds[var].dtype.itemsize
                    bytes_per_chunk = 487 * 200 * 300 * dtype_bytes
                    mib = bytes_per_chunk / (1024**2)
                    print(f"  • Chunk shape: (487,200,300) → {mib:.2f} MiB")
                mode = 'w' if first else 'a'
                append_dim = None if first else 'time'
                print(f"  Writing mode={mode} to {target_store}")
                ds[[var]].to_zarr(target_store,
                    mode=mode,
                    append_dim=append_dim,
                    consolidated=first)
                first = False
                print("data written to zarr")
                ds.close()
            for f in files:
                f.close()
                # 4) nudge Python to free any dangling HDF5 pointers
                gc.collect()

    logging.info("Finished recipe.py")
    # Clean up
    client.close()
    cluster.close()

if __name__ == "__main__":
    main()

