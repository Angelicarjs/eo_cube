# Use the official Python base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    wget \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    libgeos-dev \
    libproj-dev \
    proj-bin \
    libhdf5-dev \
    libnetcdf-dev \
    libgfortran5 \
    libblas-dev \
    liblapack-dev \
    libatlas-base-dev \
    gfortran \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Create a non-root user
RUN useradd -m -s /bin/bash jovyan
RUN mkdir -p /workspace && chown jovyan:jovyan /workspace

# Switch to the jovyan user
USER jovyan
WORKDIR /workspace

# Copy requirements file
COPY --chown=jovyan:jovyan requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Install additional packages for Earth observation
RUN pip install --no-cache-dir --user \
    contextily \
    hvplot \
    folium \
    earthpy \
    rasterio \
    fiona \
    shapely \
    pyproj \
    cartopy \
    netcdf4 \
    h5py \
    bokeh \
    panel \
    holoviews \
    datashader \
    dask-geopandas \
    xarray-spatial \
    rio-cogeo \
    rio-stac \
    pystac \
    pystac-client \
    planetary-computer \
    stackstac \
    odc-stac \
    odc-geo \
    odc-algo \
    odc-ui \
    odc-dscache \
    odc-aws \
    odc-aio \
    odc-cloud \
    odc-dtools \
    odc-io \
    odc-stats \
    odc-thredds \
    odc-xarray \
    odc-zarr \
    odc-geom \
    odc-dhash \
    odc-dtools \
    odc-io \
    odc-stats \
    odc-thredds \
    odc-xarray \
    odc-zarr \
    odc-geom \
    odc-dhash \
    odc-dtools \
    odc-io \
    odc-stats \
    odc-thredds \
    odc-xarray \
    odc-zarr \
    odc-geom \
    odc-dhash

# Set the PATH to include user-installed packages
ENV PATH="/home/jovyan/.local/bin:$PATH"

# Expose ports
EXPOSE 8888 8786 8787

# Default command
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"] 