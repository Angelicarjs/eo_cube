# %%
import os
import platform

def running_in_docker():
    """
    Check if running inside a Docker container using multiple methods
    """
    # Method 1: Check for /.dockerenv file (Linux containers)
    if os.path.exists("/.dockerenv"):
        return True
    
    # Method 2: Check cgroup (Linux only)
    if platform.system() == "Linux":
        try:
            with open("/proc/1/cgroup", "r") as f:
                content = f.read()
                if "docker" in content or "kubepods" in content:
                    return True
        except (FileNotFoundError, PermissionError, OSError):
            pass
    
    # Method 3: Check environment variables
    if os.environ.get("DOCKER_CONTAINER") == "true":
        return True
    
    # Method 4: Check for Docker-specific environment variables
    if any(var.startswith("DOCKER_") for var in os.environ.keys()):
        return True
    
    # Method 5: Check hostname (Docker containers often have random hostnames)
    try:
        hostname = os.uname().nodename
        if hostname and len(hostname) == 12 and hostname.isalnum():
            return True
    except (AttributeError, OSError):
        pass
    
    # Method 6: Check for Docker Desktop on Windows
    if platform.system() == "Windows":
        try:
            import subprocess
            result = subprocess.run(["docker", "info"], capture_output=True, text=True)
            if result.returncode == 0:
                # Docker is available, but we're not necessarily inside a container
                # Check if we're in a WSL environment that might be Docker
                if "WSL" in platform.platform():
                    return True
        except (FileNotFoundError, subprocess.SubprocessError):
            pass
    
    return False

print("¿Dentro de Docker?", running_in_docker())
print("Sistema operativo:", platform.system())
print("Plataforma:", platform.platform())
print("Variables de entorno Docker:", [k for k in os.environ.keys() if k.startswith("DOCKER_")])



# %%
import numpy as np
import xarray
import contextily
import geopandas
import stackstac
import pystac_client
import planetary_computer

# Abrimos el catálogo STAC de Planetary Computer
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

# Definimos el bounding box aproximado para el embalse de Chuza
# [lon_min, lat_min, lon_max, lat_max]
bbox =[-73.710, 4.600, -73.695, 4.615] 

# Realizamos la búsqueda en la colección de Sentinel-2 L2A
search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=bbox,
    datetime="2015-01-01/2025-01-01",
    query={"eo:cloud_cover": {"lt": 10}}
)

# Getting the metadata
items = list(search.item_collection())


# %%
print(len(items))

# %%
data = (
    stackstac.stack(
        items,
        assets=["B03", "B08"],  # red, green, blue
        chunksize=4096,
        resolution=100,
        epsg=32618
    )
    .where(lambda x: x > 0, other=np.nan)  # sentinel-2 uses 0 as nodata
    .assign_coords(band=lambda x: x.common_name.rename("band"))  # use common names
)
data

# %%
search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=bbox,
    datetime="2015-01-01/2025-01-01",
    query={"eo:cloud_cover": {"lt": 10}}
)

items = search.item_collection()

df = geopandas.GeoDataFrame.from_features(items.to_dict(), crs="epsg:4326")

ax = df[["geometry", "datetime", "s2:mgrs_tile", "eo:cloud_cover"]].plot(
    facecolor="none", figsize=(12, 6)
)
contextily.add_basemap(
    ax, crs=df.crs.to_string(), source=contextily.providers.Esri.NatGeoWorldMap
);

# %%
green = data.sel(band="green")
nir = data.sel(band="nir")

ndwi = (green - nir) / (green + nir)


# %%
import hvplot.xarray  # esto registra el método .hvplot()

# Suponiendo que ndwi es un DataArray de dimensiones: (time, y, x)
ndwi.hvplot(
    x='x',
    y='y',
    groupby='time',  # esto habilita el slider por fecha
    cmap='BrBG',
    clim=(-1, 1),
    frame_width=500,
    frame_height=400,
    title='NDWI dinámico por fecha'
)

# %%
import matplotlib.pyplot as plt

ndwi.sel(time=ndwi.time[2]).plot(cmap="BrBG", vmin=-1, vmax=1)
plt.title("NDWI")
plt.show()


# %%
import geopandas as gpd
import shapely.geometry
import rioxarray
import numpy as np
from rasterio.features import geometry_mask

# 1. Crear polígono
polygon = shapely.geometry.Polygon([
    (-73.782570942059508, 4.646643266714992),
    (-73.673135584114817, 4.647268939510189),
    (-73.675646529230207, 4.554246194913797),
    (-73.782989432912061, 4.555080533452189),
    (-73.782570942059508, 4.646643266714992),
])

# 2. Crear GeoDataFrame y asegurar CRS (WGS84, lat/lon)
gdf = gpd.GeoDataFrame({"geometry": [polygon]}, crs="EPSG:4326")

# 3. Reproyectar al CRS de NDWI si es necesario
if ndwi.rio.crs is None:
    ndwi.rio.write_crs("EPSG:4326", inplace=True)

gdf_proj = gdf.to_crs(ndwi.rio.crs)

# 4. Rasterizar la máscara del polígono al shape de NDWI
mask = geometry_mask(
    geometries=gdf_proj.geometry,
    out_shape=(ndwi.sizes['y'], ndwi.sizes['x']),
    transform=ndwi.rio.transform(),
    invert=True
)

# 5. Aplicar la máscara al NDWI
ndwi_masked = ndwi.where(mask)


# %%
ndwi_masked

# %%
print("CRS NDWI:", ndwi.rio.crs)
print("CRS GDF:", gdf.crs)
print("CRS reproyectado:", gdf_proj.crs)


# %%
plt.imshow(mask, cmap="Blues")
plt.title("Máscara rasterizada del embalse")
plt.show()


# %%
ndwi_masked.sel(time=ndwi_masked.time[2]).plot(cmap="BrBG", vmin=-1, vmax=1)
plt.title("NDWI")
plt.show()

# %%
pixel_counts = []
dates = []

for t in ndwi.time.values:
    try:
        ndwi_t = ndwi.sel(time=t).compute()
        water_mask = ndwi_t > 0.3
        count = water_mask.sum().item()  # total de pixeles de agua
        print(count)
        #pixel_counts.append(count)
        #dates.append(t)
    except Exception as e:
        print(f"Error con fecha {t}: {e}")
        continue


# %%
import numpy as np

pixel_counts = []
dates = []

for t in ndwi.time.values:
    try:
        ndwi_t = ndwi.sel(time=t).compute()
        water_mask = ndwi_t > 0.3
        count = water_mask.sum().item()  # total de pixeles de agua
        pixel_counts.append(count)
        dates.append(t)
    except Exception as e:
        print(f"Error con fecha {t}: {e}")
        continue

import pandas as pd
import matplotlib.pyplot as plt

dates = pd.to_datetime(dates)
years = dates.year

plt.figure(figsize=(10, 5))
plt.plot(years, pixel_counts, marker='o')
plt.xlabel("Año")
plt.ylabel("Número de píxeles de agua")
plt.title("Evolución anual de área de agua (NDWI > 0.3)")
plt.grid()
plt.show()



# %%
import matplotlib.pyplot as plt
import pandas as pd

# Primero computamos para obtener los valores concretos
counts = water_pixel_counts.compute()

# Extraemos los años de la coordenada time
years = pd.to_datetime(counts.time.values).year

# Graficamos
plt.figure(figsize=(10, 5))
plt.plot(years, counts.values, marker='o', linestyle='-')
plt.xlabel('Año')
plt.ylabel('Número de píxeles clasificados como agua')
plt.title('Evolución anual del área de agua (pixeles NDWI > 0.3)')
plt.grid(True)
plt.show()


# %%
import folium

# Coordenadas del centro del bbox
center_lat = (4.5980 + 4.6160) / 2
center_lon = (-73.7105 + -73.6985) / 2

# Crear el mapa centrado en el embalse
m = folium.Map(location=[center_lat, center_lon], zoom_start=14, tiles="CartoDB Positron")

# Añadir el bbox como un rectángulo al mapa
bbox = [ -73.782570942059508, 4.646643266714992 ], [ -73.673135584114817, 4.647268939510189 ], [ -73.675646529230207, 4.554246194913797 ], [ -73.782989432912061, 4.555080533452189 ], [ -73.782570942059508, 4.646643266714992 ]
folium.Rectangle(
    bounds=[[bbox[1], bbox[0]], [bbox[3], bbox[2]]],
    color="blue",
    fill=True,
    fill_opacity=0.2,
    tooltip="Embalse del Chuza"
).add_to(m)

# Mostrar el mapa
m

def load_data(item: pystac_client.Item, assets: list[str], chunksize: int, resolution: int, epsg: int) -> xarray.DataArray:
    # load the data from a datacube 
    cube = stackstac.stack(
        [item],
        assets=assets,
        chunksize=chunksize,
        resolution=resolution,
        epsg=epsg
    )
    return cube

def main():
    # load the data from a datacube 
    item = pystac_client.Item.from_dict(items[0])
    cube = load_data(item = item, assets = ["B03", "B08"], chunksize=4096, resolution=100, epsg=32618)
    return cube


# %%
if __name__ == "__main__":
    cube = main()
    print(cube)

# %%
