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

#------------
# 2. Connect to SFTP and S3
#------------
client = Client(n_workers=16)

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

#------------
# 3. Define variables and years
#------------
years = range(1980, 2024)
base_dir = "data/v4.2a/daily"
keywords = ["SMs", "Ei", "E", "H", "Et", "Ew", "Ep_rad", "Ep", "Ec", "SMrz", "Es", "Eb", "Ep_aero", "S"]

initialized = False  # Tracks whether the Zarr store has been initialized

#------------
# 4. Loop through variables and write to Zarr
#------------
for var in keywords:
    paths = []

    #------------
    # 4.1 Gather file paths for this variable
    #------------
    for year in years:
        path = f"{base_dir}/{year}/{var}_{year}_GLEAM_v4.2a.nc"
        try:
            sftp_fs.info(path)
            paths.append(path)
        except FileNotFoundError:
            print(f"⚠️ Missing: {path}")

    #------------
    # 4.2 If files exist, open and write to Zarr
    #------------
    if paths:
        open_files = [sftp_fs.open(p) for p in paths]
        ds = xr.open_mfdataset(
            open_files,
            engine="h5netcdf",
            combine="by_coords",
            parallel=True,
            chunks={"time": 365, "lat": 180, "lon": 360}
        )

        print(f"✅ Ready to write: {var}")

        mode = "w" if not initialized else "a"
        ds[[var]].chunk({"time": 200, "lat": 180, "lon": 360}).to_zarr(
            mapper, mode=mode, consolidated=False, zarr_format=2
        )

        print(f"📦 Written {var} with mode '{mode}'")
        initialized = True
    else:
        print(f"🚫 Skipped: {var}")

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
