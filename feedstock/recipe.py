#------------
# 1. Imports and setup
#------------
import fsspec
import s3fs
import xarray as xr
import warnings
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")
from distributed import Client 
import time
import dask
#------------
# 2. Connect to SFTP and S3
#------------
client = Client()

sftp_fs = fsspec.filesystem(
    "sftp", 
    host="hydras.ugent.be", 
    port=2225,
    username="gleamuser", 
    password="GLEAM4#h-cel_924"
)

s3_fs = s3fs.S3FileSystem(
    anon=True, 
    client_kwargs={"endpoint_url": "https://nyu1.osn.mghpcc.org"}
)

mapper = s3_fs.get_mapper("leap-pangeo-pipeline/GLEAM/GLEAM.zarr")
zarr_store_path="leap-pangeo-pipeline/GLEAM/GLEAM.zarr"
#------------
# 3. Define variables and years
#------------
years = range(1980, 2024)
base_dir = "data/v4.2a/daily"
keywords = ["SMs", "Ei", "E", "H", "Et", "Ew", "Ep_rad", "Ep", "Ec", "SMrz", "Es", "Eb", "Ep_aero", "S"]
print(f"variables are {variables}")
# Main loop
for year in years:
    print(f"\n🕒 Starting processing for year {year}...")
    start_time = time.time()

    datasets = []
    for var in variables:
        print(f"🔍 Processing variable: {var}")
        var_datasets = []
        remote_path = f"{base_dir}/{year}/{var}_{year}_GLEAM_v4.2a.nc"

        try:
            if sftp_fs.exists(remote_path):
                print(f"📂 Found: {remote_path}")
                f = sftp_fs.open(remote_path, mode="rb")
                ds = xr.open_dataset(f, engine="h5netcdf")
                print(f"📐 Dimensions: {ds.dims}")
                var_datasets.append(ds[[var]])
        except Exception as e:
            print(f"⚠️ Skipping {remote_path}: {e}")

        if var_datasets:
            combined_var = var_datasets[0] if len(var_datasets) == 1 else xr.concat(var_datasets, dim="time")
            combined_var = combined_var.chunk({"time": -1, "lat": 180, "lon": 360})
            datasets.append(combined_var)
            print(f"✅ Added {var} to dataset list with chunking.")

    # Merge and write to Zarr
    ds_merged = xr.merge(datasets)
    with dask.config.set(scheduler="synchronous"):
        if year == years[0]:
            ds_merged.to_zarr(zarr_store_path, mode="w", consolidated=True)
        else:
            ds_merged.to_zarr(zarr_store_path, mode="a", consolidated=True, append_dim="time")
        print(f"💾 Zarr write complete for year {year}")

    elapsed = time.time() - start_time
    print(f"✅ Year {year} completed in {elapsed:.2f} seconds.")

#------------
# 5. Final metadata consolidation
#------------
ds=xr.open_zarr(mapper).to_zarr(mapper, mode="a", consolidated=True)

n = len(keywords)
cols = 4
rows = (n + cols - 1) // cols

fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 4 * rows), constrained_layout=True)

for i, key in enumerate(keywords):
    ax = axes.flat[i]
    
    if key in ds:
        slice_2d = ds[key].isel(time=0)
        slice_2d.plot(ax=ax, cmap="viridis", add_colorbar=False)
        ax.set_title(key)
        ax.set_xlabel("")
        ax.set_ylabel("")
    else:
        ax.set_visible(False)

plt.suptitle("GLEAM Variables on First Day", fontsize=16)
plt.show()
