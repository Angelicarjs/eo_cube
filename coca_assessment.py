from pystac_client import Client
from planetary_computer import sign
from stackstac import stack
import stackstac
import pyproj
import os


os.environ["PROJ_DATA"] = r"C:/Users/angelica.moreno/AppData/Local/anaconda3/envs/eo_datacubes/Lib/site-packages/pyproj/proj_dir/share/proj"



# Conectar al catálogo STAC
catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=[-76.5, 1.5, -75.5, 2.5],  # Putumayo (ajusta según tu zona de estudio)
    datetime="2022-01-01/2022-12-31",
    query={"eo:cloud_cover": {"lt": 20}}
)

items = list(search.items())

#print(items)

signed_items = [sign(item) for item in items]

#print(signed_items[0].assets["B04"].to_dict())

cube = stackstac.stack(
    signed_items,
    assets=["B04", "B08"],
    epsg=32618  # CRS común para UTM zona 18 norte (usado en Colombia)
)

# Paso 7: Persistir para trabajar eficientemente
cube = cube.persist()

#cube = cube.chunk({"x": 512, "y": 512, "time": 1})  # puedes ajustar a 256 si falla

# no persist, trabaja con el lazy cube
#print(cube)



""" cube = stack(
    signed_items,
    assets=["B04", "B08"],  # Rojo y NIR para NDVI
    resolution=10,
    chunksize=2048,
    bounds_latlon=(-76.5, 1.5, -75.5, 2.5),
)
 """
