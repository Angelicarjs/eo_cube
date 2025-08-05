
"""
Water Analysis Framework using Microsoft Planetary Computer
==========================================================

This script provides a generic framework for analyzing water pixels over time
using Sentinel-2 data from Microsoft Planetary Computer.

Features:
- Data retrieval from Planetary Computer
- Water index calculation (NDWI, MNDWI)
- Temporal analysis of water extent
- Visualization and statistics
- Configurable parameters for different study areas
"""

import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xarray as xr
import geopandas as gpd
import shapely.geometry
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
import logging
import dask
from dask.distributed import Client, LocalCluster
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import time

# Remote sensing libraries
import stackstac
import pystac_client
import planetary_computer
import rioxarray
from rasterio.features import geometry_mask

# Visualization
import contextily
import folium
import hvplot.xarray

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('water_analysis.log')
    ]
)
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

def configure_dask_for_performance():
    """
    Configure Dask for optimal performance with parallel processing.
    """
    # Get number of CPU cores
    n_cores = multiprocessing.cpu_count()
    n_workers = min(n_cores, 8)  # Limit to 8 workers to avoid memory issues
    
    logger.info(f"🖥️  Detected {n_cores} CPU cores, using {n_workers} workers")
    
    # Configure Dask
    dask.config.set({
        'array.chunk-size': '128MB',
        'distributed.worker.memory.target': 0.8,  # Use 80% of available memory
        'distributed.worker.memory.spill': 0.9,   # Spill to disk at 90%
        'distributed.worker.memory.pause': 0.95,  # Pause at 95%
        'distributed.worker.memory.terminate': 0.98,  # Terminate at 98%
        'distributed.comm.timeouts.connect': '60s',
        'distributed.comm.timeouts.tcp': '600s',
        'distributed.scheduler.work-stealing': True,
        'distributed.scheduler.bandwidth': 1000000000,  # 1GB/s
    })
    
    # Create local cluster
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=2,
        memory_limit='4GB',  # 4GB per worker
        local_directory='/tmp/dask-worker-space'
    )
    
    # Create client
    client = Client(cluster)
    
    logger.info(f"🚀 Dask cluster started with {n_workers} workers")
    logger.info(f"📊 Dashboard available at: {client.dashboard_link}")
    
    return client

def cleanup_dask_client(client):
    """
    Clean up Dask client and cluster.
    """
    if client is not None:
        client.close()
        logger.info("🧹 Dask client closed")

def print_performance_summary(start_time, end_time, data_shape):
    """
    Print a summary of performance metrics.
    """
    total_time = end_time - start_time
    total_pixels = np.prod(data_shape)
    pixels_per_second = total_pixels / total_time if total_time > 0 else 0
    
    logger.info("\n" + "="*60)
    logger.info("📊 PERFORMANCE SUMMARY")
    logger.info("="*60)
    logger.info(f"⏱️  Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    logger.info(f"📊 Total pixels processed: {total_pixels:,}")
    logger.info(f"⚡ Processing speed: {pixels_per_second:,.0f} pixels/second")
    logger.info(f"📈 Data shape: {data_shape}")
    logger.info("="*60)


class WaterAnalyzer:
    """
    A class for analyzing water extent using satellite imagery from Planetary Computer.
    """
    
    def __init__(self, 
                 collection: str = "sentinel-2-l2a",
                 assets: List[str] = None,
                 chunksize: int = 2048,
                 resolution: int = 100,
                 epsg: int = 32618,
                 enable_parallel: bool = True,
                 n_workers: int = None):
        """
        Initialize the WaterAnalyzer.
        
        Parameters:
        -----------
        collection : str
            STAC collection to use (default: sentinel-2-l2a)
        assets : List[str]
            Band assets to load (default: ["B03", "B08", "B11"])
        chunksize : int
            Chunk size for dask arrays
        resolution : int
            Resolution in meters
        epsg : int
            EPSG code for projection
        """
        self.collection = collection
        self.assets = assets or ["B03", "B08", "B11"]  # Green, NIR, SWIR
        self.chunksize = chunksize
        self.resolution = resolution
        self.epsg = epsg
        self.enable_parallel = enable_parallel
        self.n_workers = n_workers or min(multiprocessing.cpu_count(), 8)
        
        # Initialize catalog
        self.catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
        
        # Data storage
        self.data = None
        self.ndwi = None
        self.mndwi = None
        self.items = None
        self.dask_client = None
        
        logger.info(f"Initialized WaterAnalyzer with collection: {collection}")
        if self.enable_parallel:
            logger.info(f"🚀 Parallel processing enabled with {self.n_workers} workers")
    
    def start_dask_client(self):
        """
        Start Dask client for parallel processing.
        """
        if self.enable_parallel and self.dask_client is None:
            logger.info("🚀 Starting Dask client for parallel processing...")
            self.dask_client = configure_dask_for_performance()
            return self.dask_client
        return None
    
    def stop_dask_client(self):
        """
        Stop Dask client.
        """
        if self.dask_client is not None:
            cleanup_dask_client(self.dask_client)
            self.dask_client = None
    
    def search_data(self,
                   bbox: List[float],
                   start_date: str = "2020-01-01",
                   end_date: str = "2023-12-31",
                   max_cloud_cover: float = 20.0,
                   max_items: int = 50) -> List:
        """
        Search for satellite data in the specified area and time period.
        
        Parameters:
        -----------
        bbox : List[float]
            Bounding box [lon_min, lat_min, lon_max, lat_max]
        start_date : str
            Start date in YYYY-MM-DD format
        end_date : str
            End date in YYYY-MM-DD format
        max_cloud_cover : float
            Maximum cloud cover percentage
        max_items : int
            Maximum number of items to retrieve
            
        Returns:
        --------
        List of STAC items
        """
        logger.info(f"🔍 Searching for data from {start_date} to {end_date}")
        logger.info(f"📍 Bounding box: {bbox}")
        logger.info(f"☁️  Maximum cloud cover: {max_cloud_cover}%")
        logger.info(f"📊 Maximum items to retrieve: {max_items}")
        
        # Perform search
        logger.info("🔎 Executing STAC search query...")
        search = self.catalog.search(
            collections=[self.collection],
            bbox=bbox,
            datetime=f"{start_date}/{end_date}",
            query={"eo:cloud_cover": {"lt": max_cloud_cover}},
            limit=max_items
        )
        
        self.items = list(search.item_collection())
        logger.info(f"✅ Found {len(self.items)} items")
        
        if len(self.items) > 0:
            # Log some details about the found items
            first_item = self.items[0]
            last_item = self.items[-1]
            logger.info(f"📅 Date range: {first_item.datetime.strftime('%Y-%m-%d')} to {last_item.datetime.strftime('%Y-%m-%d')}")
            
            # Calculate average cloud cover
            cloud_covers = [item.properties.get('eo:cloud_cover', 0) for item in self.items]
            avg_cloud_cover = sum(cloud_covers) / len(cloud_covers)
            logger.info(f"☁️  Average cloud cover: {avg_cloud_cover:.1f}%")
        
        if len(self.items) == 0:
            raise ValueError("No data found for the specified parameters")
        
        return self.items
    
    def load_data(self) -> xr.DataArray:
        """
        Load and stack the satellite data.
        
        Returns:
        --------
        xarray.DataArray with the stacked data
        """
        if self.items is None or len(self.items) == 0:
            raise ValueError("No items available. Run search_data() first.")
        
        logger.info(f"📥 Loading data for {len(self.items)} items")
        logger.info(f"🔧 Configuration: chunksize={self.chunksize}, resolution={self.resolution}m, EPSG={self.epsg}")
        logger.info(f"🎯 Target assets: {self.assets}")
        
        # Start Dask client if parallel processing is enabled
        if self.enable_parallel:
            self.start_dask_client()
        
        # Optimize chunking for parallel processing
        optimal_chunksize = min(self.chunksize, 1024)  # Smaller chunks for better parallelization
        logger.info(f"🔧 Using optimized chunk size: {optimal_chunksize}")
        
        # Stack the data with optimized settings
        logger.info("🔄 Stacking satellite data with parallel optimization...")
        start_time = time.time()
        
        # Prepare stackstac parameters
        stack_params = {
            'items': self.items,
            'assets': self.assets,
            'chunksize': optimal_chunksize,
            'resolution': self.resolution,
        }
        
        # Only add epsg if specified
        if self.epsg is not None:
            stack_params['epsg'] = self.epsg
        
        self.data = (
            stackstac.stack(**stack_params)
            .where(lambda x: x > 0, other=np.nan)  # Sentinel-2 uses 0 as nodata
            .assign_coords(band=lambda x: x.common_name.rename("band"))
            .chunk({
                'time': 1,  # Process one time step at a time
                'band': -1,  # Keep all bands together
                'x': optimal_chunksize,
                'y': optimal_chunksize
            })
        )
        
        load_time = time.time() - start_time
        logger.info(f"⚡ Data loading completed in {load_time:.2f} seconds")
        
        logger.info(f"✅ Data loaded successfully!")
        logger.info(f"📊 Data shape: {self.data.shape}")
        logger.info(f"🎨 Available bands: {list(self.data.band.values)}")
        
        # Log data size information
        total_pixels = np.prod(self.data.shape)
        data_size_gb = self.data.nbytes / (1024**3)
        logger.info(f"💾 Total pixels: {total_pixels:,}")
        logger.info(f"💾 Estimated data size: {data_size_gb:.2f} GB")
        
        return self.data
    
    def calculate_water_indices(self) -> Dict[str, xr.DataArray]:
        """
        Calculate water indices (NDWI and MNDWI).
        
        Returns:
        --------
        Dictionary containing NDWI and MNDWI arrays
        """
        if self.data is None:
            raise ValueError("No data loaded. Run load_data() first.")
        
        logger.info("🌊 Calculating water indices...")
        logger.info("📐 Using bands: Green (B03), NIR (B08)")
        
        # Get bands
        logger.info("🎯 Extracting individual bands...")
        green = self.data.sel(band="green")
        nir = self.data.sel(band="nir")
        
        logger.info(f"✅ Green band shape: {green.shape}")
        logger.info(f"✅ NIR band shape: {nir.shape}")
        
        # Calculate water indices with parallel optimization
        start_time = time.time()
        
        logger.info("🧮 Calculating NDWI: (Green - NIR) / (Green + NIR)")
        self.ndwi = (green - nir) / (green + nir)
        
        # Note: MNDWI requires SWIR band, so we'll skip it for now
        logger.info("⚠️  MNDWI skipped - requires SWIR band (B11)")
        self.mndwi = None
        
        # Compute indices in parallel if Dask client is available
        if self.enable_parallel and self.dask_client is not None:
            logger.info("🚀 Computing water indices in parallel...")
            self.ndwi = self.ndwi.compute()
            if self.mndwi is not None:
                self.mndwi = self.mndwi.compute()
        
        indices_time = time.time() - start_time
        logger.info(f"⚡ Water indices calculation completed in {indices_time:.2f} seconds")
        
        # Log statistics about the calculated indices
        logger.info("📊 Water indices statistics:")
        logger.info(f"   NDWI range: {float(self.ndwi.min()):.3f} to {float(self.ndwi.max()):.3f}")
        logger.info(f"   NDWI mean: {float(self.ndwi.mean()):.3f}")
        if self.mndwi is not None:
            logger.info(f"   MNDWI range: {float(self.mndwi.min()):.3f} to {float(self.mndwi.max()):.3f}")
            logger.info(f"   MNDWI mean: {float(self.mndwi.mean()):.3f}")
        
        logger.info("✅ Water indices calculated successfully")
        
        result = {"ndwi": self.ndwi}
        if self.mndwi is not None:
            result["mndwi"] = self.mndwi
        return result
    
    def save_ndwi_data(self, filename: str) -> None:
        """
        Save NDWI data and metadata to a numpy compressed file.
        
        Parameters:
        -----------
        filename : str
            Output filename (should end with .npz)
        """
        if self.ndwi is None:
            raise ValueError("NDWI not calculated. Run calculate_water_indices() first.")
        
        logger.info(f"💾 Saving NDWI data to {filename}...")
        
        # Convert to numpy arrays and save metadata
        ndwi_data = self.ndwi.values  # Shape: (time, y, x)
        time_coords = self.ndwi.time.values
        x_coords = self.ndwi.x.values
        y_coords = self.ndwi.y.values
        
        # Save as compressed numpy file with simple metadata
        np.savez_compressed(
            filename,
            ndwi_data=ndwi_data,
            time_coords=time_coords,
            x_coords=x_coords,
            y_coords=y_coords,
            crs=str(self.ndwi.rio.crs) if hasattr(self.ndwi, 'rio') else "Unknown",
            description='NDWI data from Sentinel-2 analysis',
            date_created=str(pd.Timestamp.now()),
            data_shape=ndwi_data.shape,
            time_start=str(time_coords[0]),
            time_end=str(time_coords[-1]),
            total_images=len(time_coords)
        )
        
        logger.info(f"✅ Data saved successfully!")
        logger.info(f"   📊 Data shape: {ndwi_data.shape}")
        logger.info(f"   📅 Time range: {time_coords[0]} to {time_coords[-1]}")
        logger.info(f"   💾 File size: {os.path.getsize(filename) / (1024*1024):.2f} MB")
    
    def load_ndwi_data(self, filename: str) -> Dict[str, Any]:
        """
        Load NDWI data from a numpy compressed file.
        
        Parameters:
        -----------
        filename : str
            Input filename (.npz file)
            
        Returns:
        --------
        Dictionary containing data and metadata
        """
        logger.info(f"📂 Loading NDWI data from {filename}...")
        
        # Load the data
        data = np.load(filename)
        
        # Extract data and metadata
        ndwi_data = data['ndwi_data']
        time_coords = data['time_coords']
        x_coords = data['x_coords']
        y_coords = data['y_coords']
        
        # Create metadata dictionary from individual fields
        metadata = {
            'crs': str(data['crs']),
            'description': str(data['description']),
            'date_created': str(data['date_created']),
            'data_shape': data['data_shape'],
            'time_start': str(data['time_start']),
            'time_end': str(data['time_end']),
            'total_images': int(data['total_images'])
        }
        
        logger.info(f"✅ Data loaded successfully!")
        logger.info(f"   📊 Data shape: {ndwi_data.shape}")
        logger.info(f"   📅 Time range: {metadata['time_start']} to {metadata['time_end']}")
        
        return {
            'ndwi_data': ndwi_data,
            'time_coords': time_coords,
            'x_coords': x_coords,
            'y_coords': y_coords,
            'metadata': metadata
        }
    
    def get_data_statistics(self) -> Dict[str, Any]:
        """
        Get basic statistics about the loaded data.
        
        Returns:
        --------
        Dictionary with data statistics
        """
        if self.data is None:
            raise ValueError("No data loaded. Run load_data() first.")
        
        stats = {
            "shape": self.data.shape,
            "dimensions": dict(self.data.sizes),
            "bands": list(self.data.band.values),
            "time_range": {
                "start": str(self.data.time.min().values),
                "end": str(self.data.time.max().values),
                "total_dates": len(self.data.time)
            },
            "spatial_extent": {
                "x_min": float(self.data.x.min()),
                "x_max": float(self.data.x.max()),
                "y_min": float(self.data.y.min()),
                "y_max": float(self.data.y.max())
            },
            "crs": str(self.data.rio.crs) if hasattr(self.data, 'rio') else "Unknown"
        }
        
        return stats
    
    def analyze_water_extent(self, 
                           water_index: str = "ndwi",
                           threshold: float = 0.3,
                           mask_geometry: Optional[shapely.geometry.Polygon] = None) -> pd.DataFrame:
        """
        Analyze water extent over time.
        
        Parameters:
        -----------
        water_index : str
            Which water index to use ("ndwi" or "mndwi")
        threshold : float
            Threshold for water classification
        mask_geometry : Optional[shapely.geometry.Polygon]
            Optional geometry to mask the analysis area
            
        Returns:
        --------
        DataFrame with temporal water extent data
        """
        if water_index == "ndwi" and self.ndwi is None:
            raise ValueError("NDWI not calculated. Run calculate_water_indices() first.")
        elif water_index == "mndwi" and self.mndwi is None:
            raise ValueError("MNDWI not calculated. Run calculate_water_indices() first.")
        
        water_data = self.ndwi if water_index == "ndwi" else self.mndwi
        
        logger.info(f"🌊 Analyzing water extent using {water_index.upper()} with threshold {threshold}")
        logger.info(f"📊 Data shape: {water_data.shape}")
        logger.info(f"📅 Time steps to process: {len(water_data.time)}")
        
        # Apply mask if provided
        if mask_geometry is not None:
            logger.info("🎭 Applying geometry mask...")
            water_data = self._apply_geometry_mask(water_data, mask_geometry)
        
        # Calculate water pixels for each time step using parallel processing
        logger.info("🔄 Processing time steps with parallel optimization...")
        start_time = time.time()
        
        if self.enable_parallel and self.dask_client is not None:
            # Use Dask for parallel processing
            logger.info("🚀 Using Dask parallel processing...")
            results = self._analyze_water_extent_parallel(water_data, threshold)
        else:
            # Fallback to sequential processing
            logger.info("🔄 Using sequential processing...")
            results = self._analyze_water_extent_sequential(water_data, threshold)
        
        analysis_time = time.time() - start_time
        logger.info(f"⚡ Water extent analysis completed in {analysis_time:.2f} seconds")
        
        return pd.DataFrame(results)
    
    def _analyze_water_extent_parallel(self, water_data, threshold):
        """
        Analyze water extent using parallel processing with Dask.
        """
        logger.info("🚀 Starting parallel water extent analysis...")
        
        # Create a function to process a single time step
        def process_time_step(time_idx):
            try:
                t = water_data.time.values[time_idx]
                water_slice = water_data.isel(time=time_idx)
                
                # Check if water_slice is None or empty
                if water_slice is None:
                    logger.warning(f"⚠️  Empty data for time step {time_idx}")
                    return None
                
                # Compute the slice
                water_slice = water_slice.compute()
                
                # Create water mask
                water_mask = water_slice > threshold
                
                # Count water pixels
                water_pixels = water_mask.sum().item()
                total_pixels = (~np.isnan(water_slice)).sum().item()
                
                # Calculate percentage
                water_percentage = (water_pixels / total_pixels * 100) if total_pixels > 0 else 0
                
                return {
                    'date': pd.to_datetime(t),
                    'water_pixels': water_pixels,
                    'total_pixels': total_pixels,
                    'water_percentage': water_percentage,
                    'water_index_mean': float(water_slice.mean()),
                    'water_index_std': float(water_slice.std())
                }
            except Exception as e:
                logger.warning(f"⚠️  Error processing time step {time_idx}: {e}")
                return None
        
        # Process time steps in parallel
        results = []
        time_indices = list(range(len(water_data.time)))
        
        # Use ThreadPoolExecutor for I/O bound operations
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            # Submit all tasks
            future_to_idx = {executor.submit(process_time_step, idx): idx for idx in time_indices}
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                    completed += 1
                    
                    # Log progress
                    if completed % 5 == 0 or completed == len(time_indices):
                        progress = (completed / len(time_indices)) * 100
                        logger.info(f"📈 Parallel progress: {progress:.1f}% ({completed}/{len(time_indices)})")
                        
                except Exception as e:
                    logger.error(f"❌ Error in parallel processing for time step {idx}: {e}")
        
        # Sort results by date
        results.sort(key=lambda x: x['date'])
        return results
    
    def _analyze_water_extent_sequential(self, water_data, threshold):
        """
        Analyze water extent using sequential processing (fallback).
        """
        logger.info("🔄 Using sequential processing...")
        results = []
        
        for i, t in enumerate(water_data.time.values):
            try:
                # Log progress every 5 time steps or at the end
                if i % 5 == 0 or i == len(water_data.time.values) - 1:
                    progress = (i + 1) / len(water_data.time.values) * 100
                    logger.info(f"📈 Progress: {progress:.1f}% ({i+1}/{len(water_data.time.values)})")
                
                # Select time slice and compute
                water_slice = water_data.sel(time=t)
                if water_slice is None:
                    logger.warning(f"⚠️  Empty data for time {t}")
                    continue
                water_slice = water_slice.compute()
                
                # Create water mask
                water_mask = water_slice > threshold
                
                # Count water pixels
                water_pixels = water_mask.sum().item()
                total_pixels = (~np.isnan(water_slice)).sum().item()
                
                # Calculate percentage
                water_percentage = (water_pixels / total_pixels * 100) if total_pixels > 0 else 0
                
                results.append({
                    'date': pd.to_datetime(t),
                    'water_pixels': water_pixels,
                    'total_pixels': total_pixels,
                    'water_percentage': water_percentage,
                    'water_index_mean': float(water_slice.mean()),
                    'water_index_std': float(water_slice.std())
                })
                
            except Exception as e:
                logger.warning(f"Error processing date {t}: {e}")
                continue
        
        df = pd.DataFrame(results)
        logger.info(f"Analysis complete. Processed {len(df)} time steps")
        
        return df
    
    def _apply_geometry_mask(self, data: xr.DataArray, geometry: shapely.geometry.Polygon) -> xr.DataArray:
        """
        Apply a geometry mask to the data.
        
        Parameters:
        -----------
        data : xarray.DataArray
            Data to mask
        geometry : shapely.geometry.Polygon
            Geometry to use as mask
            
        Returns:
        --------
        Masked xarray.DataArray
        """
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame({"geometry": [geometry]}, crs="EPSG:4326")
        
        # Ensure data has CRS
        if data.rio.crs is None:
            data.rio.write_crs("EPSG:4326", inplace=True)
        
        # Reproject geometry to data CRS
        gdf_proj = gdf.to_crs(data.rio.crs)
        
        # Create mask
        mask = geometry_mask(
            geometries=gdf_proj.geometry,
            out_shape=(data.sizes['y'], data.sizes['x']),
            transform=data.rio.transform(),
            invert=True
        )
        
        # Apply mask
        return data.where(mask)
    
    def plot_temporal_analysis(self, 
                             df: pd.DataFrame,
                             save_path: Optional[str] = "images/temporal_analysis.png") -> None:
        """
        Plot temporal analysis results.
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame from analyze_water_extent()
        save_path : Optional[str]
            Path to save the plot
        """
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Plot 1: Water pixels over time
        ax1.plot(df['date'], df['water_pixels'], marker='o', linestyle='-', linewidth=2, markersize=4)
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Water Pixels')
        ax1.set_title('Water Extent Over Time')
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
        
        # Plot 2: Water percentage over time
        ax2.plot(df['date'], df['water_percentage'], marker='s', linestyle='-', 
                linewidth=2, markersize=4, color='orange')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Water Percentage (%)')
        ax2.set_title('Water Percentage Over Time')
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Plot saved to {save_path}")
        
        plt.show()
    
    def plot_water_index_map(self, 
                           water_index: str = "ndwi",
                           time_index: int = 0,
                           save_path: Optional[str] = "images/water_index_map.png") -> None:
        """
        Plot a water index map for a specific time.
        
        Parameters:
        -----------
        water_index : str
            Which water index to plot ("ndwi" or "mndwi")
        time_index : int
            Time index to plot
        save_path : Optional[str]
            Path to save the plot
        """
        if water_index == "ndwi" and self.ndwi is None:
            raise ValueError("NDWI not calculated. Run calculate_water_indices() first.")
        elif water_index == "mndwi" and self.mndwi is None:
            raise ValueError("MNDWI not calculated. Run calculate_water_indices() first.")
        
        water_data = self.ndwi if water_index == "ndwi" else self.mndwi
        
        # Select time slice
        data_slice = water_data.isel(time=time_index)
        
        # Create plot
        fig, ax = plt.subplots(figsize=(10, 8))
        
        im = data_slice.plot(
            ax=ax,
            cmap='BrBG',
            vmin=-1,
            vmax=1,
            add_colorbar=True,
            cbar_kwargs={'label': f'{water_index.upper()} Value'}
        )
        
        ax.set_title(f'{water_index.upper()} - {pd.to_datetime(data_slice.time.values).strftime("%Y-%m-%d")}')
        ax.set_xlabel('X Coordinate')
        ax.set_ylabel('Y Coordinate')
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Map saved to {save_path}")
        
        plt.show()
    
    def create_interactive_map(self, bbox: List[float]) -> folium.Map:
        """
        Create an interactive map showing the study area.
        
        Parameters:
        -----------
        bbox : List[float]
            Bounding box [lon_min, lat_min, lon_max, lat_max]
            
        Returns:
        --------
        folium.Map object
        """
        # Calculate center
        center_lat = (bbox[1] + bbox[3]) / 2
        center_lon = (bbox[0] + bbox[2]) / 2
        
        # Create map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=12,
            tiles="CartoDB Positron"
        )
        
        # Add bounding box
        folium.Rectangle(
            bounds=[[bbox[1], bbox[0]], [bbox[3], bbox[2]]],
            color="blue",
            fill=True,
            fill_opacity=0.2,
            tooltip="Study Area"
        ).add_to(m)
        
        return m

    def create_data_cube_visualizations(self, save_dir: str = "images") -> None:
        """
        Create comprehensive visualizations of the data cube.
        
        Parameters:
        -----------
        save_dir : str
            Directory to save the visualizations
        """
        if self.data is None:
            raise ValueError("No data loaded. Run load_data() first.")
        
        logger.info(f"Creating comprehensive data cube visualizations in {save_dir}")
        
        # Ensure save directory exists
        os.makedirs(save_dir, exist_ok=True)
        
        # 1. Data cube overview
        logger.info("Creating data cube overview visualization...")
        self._plot_data_cube_overview(save_dir)
        
        # 2. Band statistics
        logger.info("Creating band statistics visualization...")
        self._plot_band_statistics(save_dir)
        
        # 3. Time series overview
        logger.info("Creating time series overview...")
        self._plot_time_series_overview(save_dir)
        
        # 4. Spatial coverage
        logger.info("Creating spatial coverage visualization...")
        self._plot_spatial_coverage(save_dir)
        
        # 5. Water indices comparison (only if MNDWI is available)
        if self.ndwi is not None:
            logger.info("Creating water indices visualization...")
            if self.mndwi is not None:
                self._plot_water_indices_comparison(save_dir)
            else:
                self._plot_ndwi_only(save_dir)
        
        logger.info(f"All visualizations saved to {save_dir}/")

    def _plot_data_cube_overview(self, save_dir: str) -> None:
        """Create an overview of the data cube structure."""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Data Cube Overview', fontsize=16, fontweight='bold')
        
        # Plot 1: Data shape and dimensions
        ax1 = axes[0, 0]
        dims = list(self.data.sizes.items())
        dim_names = [d[0] for d in dims]
        dim_values = [d[1] for d in dims]
        
        bars = ax1.bar(dim_names, dim_values, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
        ax1.set_title('Data Cube Dimensions')
        ax1.set_ylabel('Size')
        ax1.set_xlabel('Dimension')
        
        # Add value labels on bars
        for bar, value in zip(bars, dim_values):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01*max(dim_values),
                    f'{value:,}', ha='center', va='bottom', fontweight='bold')
        
        # Plot 2: Time coverage
        ax2 = axes[0, 1]
        dates = pd.to_datetime(self.data.time.values)
        ax2.plot(dates, range(len(dates)), 'o-', linewidth=2, markersize=4)
        ax2.set_title('Temporal Coverage')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Time Index')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Band information
        ax3 = axes[1, 0]
        bands = list(self.data.band.values)
        band_colors = ['green', 'red', 'blue']
        ax3.bar(bands, [1]*len(bands), color=band_colors[:len(bands)])
        ax3.set_title('Available Bands')
        ax3.set_ylabel('Available')
        ax3.set_ylim(0, 1.2)
        
        # Add band descriptions
        band_descriptions = {
            'green': 'Green (B03)',
            'nir': 'Near-Infrared (B08)', 
            'swir16': 'Short-Wave IR (B11)'
        }
        for i, band in enumerate(bands):
            desc = band_descriptions.get(band, band)
            ax3.text(i, 0.6, desc, ha='center', va='center', fontweight='bold')
        
        # Plot 4: Data statistics
        ax4 = axes[1, 1]
        stats_data = []
        stats_labels = []
        
        for band in bands:
            band_data = self.data.sel(band=band)
            stats_data.extend([
                float(band_data.min()),
                float(band_data.max()),
                float(band_data.mean()),
                float(band_data.std())
            ])
            stats_labels.extend([f'{band}_min', f'{band}_max', f'{band}_mean', f'{band}_std'])
        
        ax4.bar(range(len(stats_data)), stats_data, alpha=0.7)
        ax4.set_title('Band Statistics')
        ax4.set_ylabel('Value')
        ax4.set_xticks(range(len(stats_labels)))
        ax4.set_xticklabels(stats_labels, rotation=45, ha='right')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/01_data_cube_overview.png', dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Data cube overview saved")

    def _plot_band_statistics(self, save_dir: str) -> None:
        """Create detailed band statistics visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Detailed Band Statistics', fontsize=16, fontweight='bold')
        
        bands = list(self.data.band.values)
        colors = ['green', 'red', 'blue']
        
        for i, band in enumerate(bands):
            band_data = self.data.sel(band=band)
            
            # Histogram
            ax = axes[i//2, i%2]
            band_data.plot.hist(ax=ax, bins=50, alpha=0.7, color=colors[i])
            ax.set_title(f'{band.upper()} Band Distribution')
            ax.set_xlabel('Pixel Value')
            ax.set_ylabel('Frequency')
            ax.grid(True, alpha=0.3)
            
            # Add statistics text
            mean_val = float(band_data.mean())
            std_val = float(band_data.std())
            min_val = float(band_data.min())
            max_val = float(band_data.max())
            
            stats_text = f'Mean: {mean_val:.2f}\nStd: {std_val:.2f}\nMin: {min_val:.2f}\nMax: {max_val:.2f}'
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
                   verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/02_band_statistics.png', dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Band statistics saved")

    def _plot_time_series_overview(self, save_dir: str) -> None:
        """Create time series overview visualization."""
        fig, axes = plt.subplots(3, 1, figsize=(15, 12))
        fig.suptitle('Time Series Overview', fontsize=16, fontweight='bold')
        
        bands = list(self.data.band.values)
        colors = ['green', 'red', 'blue']
        
        for i, band in enumerate(bands):
            band_data = self.data.sel(band=band)
            
            # Calculate statistics over time
            time_mean = band_data.mean(dim=['x', 'y'])
            time_std = band_data.std(dim=['x', 'y'])
            
            dates = pd.to_datetime(time_mean.time.values)
            
            ax = axes[i]
            ax.plot(dates, time_mean, 'o-', color=colors[i], linewidth=2, markersize=4, label='Mean')
            ax.fill_between(dates, time_mean - time_std, time_mean + time_std, 
                          alpha=0.3, color=colors[i], label='±1 Std')
            
            ax.set_title(f'{band.upper()} Band - Temporal Statistics')
            ax.set_xlabel('Date')
            ax.set_ylabel('Pixel Value')
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/03_time_series_overview.png', dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Time series overview saved")

    def _plot_spatial_coverage(self, save_dir: str) -> None:
        """Create spatial coverage visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Spatial Coverage Analysis', fontsize=16, fontweight='bold')
        
        # Plot 1: First time step RGB-like composite
        ax1 = axes[0, 0]
        first_slice = self.data.isel(time=0)
        
        # Create RGB composite (normalize each band)
        rgb_data = np.zeros((first_slice.sizes['y'], first_slice.sizes['x'], 3))
        for i, band in enumerate(['green', 'nir', 'swir16']):
            if band in first_slice.band.values:
                band_data = first_slice.sel(band=band).values
                # Normalize to 0-1
                band_norm = (band_data - band_data.min()) / (band_data.max() - band_data.min())
                rgb_data[:, :, i] = band_norm
        
        ax1.imshow(rgb_data)
        ax1.set_title('RGB Composite (First Time Step)')
        ax1.set_xlabel('X Pixels')
        ax1.set_ylabel('Y Pixels')
        ax1.axis('off')
        
        # Plot 2: Spatial extent
        ax2 = axes[0, 1]
        x_coords = self.data.x.values
        y_coords = self.data.y.values
        
        ax2.plot(x_coords, [y_coords[0]]*len(x_coords), 'b-', linewidth=2, label='Top')
        ax2.plot(x_coords, [y_coords[-1]]*len(x_coords), 'r-', linewidth=2, label='Bottom')
        ax2.plot([x_coords[0]]*len(y_coords), y_coords, 'g-', linewidth=2, label='Left')
        ax2.plot([x_coords[-1]]*len(y_coords), y_coords, 'orange', linewidth=2, label='Right')
        
        ax2.set_title('Spatial Extent')
        ax2.set_xlabel('X Coordinate')
        ax2.set_ylabel('Y Coordinate')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Coverage area
        ax3 = axes[1, 0]
        area_km2 = (x_coords[-1] - x_coords[0]) * (y_coords[-1] - y_coords[0]) / 1e6
        ax3.text(0.5, 0.5, f'Coverage Area:\n{area_km2:.2f} km²', 
                ha='center', va='center', fontsize=14, fontweight='bold',
                transform=ax3.transAxes)
        ax3.set_title('Study Area Coverage')
        ax3.axis('off')
        
        # Plot 4: Resolution information
        ax4 = axes[1, 1]
        x_res = (x_coords[-1] - x_coords[0]) / (len(x_coords) - 1)
        y_res = (y_coords[-1] - y_coords[0]) / (len(y_coords) - 1)
        
        info_text = f'Resolution:\nX: {x_res:.1f} m\nY: {y_res:.1f} m\n\nPixel Count:\nX: {len(x_coords)}\nY: {len(y_coords)}'
        ax4.text(0.5, 0.5, info_text, ha='center', va='center', fontsize=12,
                transform=ax4.transAxes, bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        ax4.set_title('Spatial Resolution')
        ax4.axis('off')
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/04_spatial_coverage.png', dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Spatial coverage saved")

    def _plot_water_indices_comparison(self, save_dir: str) -> None:
        """Create water indices comparison visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Water Indices Comparison', fontsize=16, fontweight='bold')
        
        # Plot 1: NDWI histogram
        ax1 = axes[0, 0]
        ndwi_values = self.ndwi.values.flatten()
        ax1.hist(ndwi_values, bins=50, alpha=0.7, color='blue', edgecolor='black')
        ax1.axvline(x=0.3, color='red', linestyle='--', linewidth=2, label='Water Threshold (0.3)')
        ax1.set_title('NDWI Distribution')
        ax1.set_xlabel('NDWI Value')
        ax1.set_ylabel('Frequency')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: MNDWI histogram
        ax2 = axes[0, 1]
        mndwi_values = self.mndwi.values.flatten()
        ax2.hist(mndwi_values, bins=50, alpha=0.7, color='green', edgecolor='black')
        ax2.axvline(x=0.3, color='red', linestyle='--', linewidth=2, label='Water Threshold (0.3)')
        ax2.set_title('MNDWI Distribution')
        ax2.set_xlabel('MNDWI Value')
        ax2.set_ylabel('Frequency')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: NDWI map (first time step)
        ax3 = axes[1, 0]
        ndwi_slice = self.ndwi.isel(time=0)
        im3 = ndwi_slice.plot(ax=ax3, cmap='BrBG', vmin=-1, vmax=1, add_colorbar=True)
        ax3.set_title(f'NDWI - {pd.to_datetime(ndwi_slice.time.values).strftime("%Y-%m-%d")}')
        
        # Plot 4: MNDWI map (first time step)
        ax4 = axes[1, 1]
        mndwi_slice = self.mndwi.isel(time=0)
        im4 = mndwi_slice.plot(ax=ax4, cmap='BrBG', vmin=-1, vmax=1, add_colorbar=True)
        ax4.set_title(f'MNDWI - {pd.to_datetime(mndwi_slice.time.values).strftime("%Y-%m-%d")}')
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/05_water_indices_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Water indices comparison saved")

    def _plot_ndwi_only(self, save_dir: str) -> None:
        """Create NDWI-only visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('NDWI Analysis', fontsize=16, fontweight='bold')
        
        # Plot 1: NDWI histogram
        ax1 = axes[0, 0]
        if self.ndwi is not None:
            ndwi_values = self.ndwi.values.flatten()
            ax1.hist(ndwi_values, bins=50, alpha=0.7, color='blue', edgecolor='black')
            ax1.axvline(x=0.3, color='red', linestyle='--', linewidth=2, label='Water Threshold (0.3)')
            ax1.set_title('NDWI Distribution')
            ax1.set_xlabel('NDWI Value')
            ax1.set_ylabel('Frequency')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
        else:
            ax1.text(0.5, 0.5, 'No NDWI data available', ha='center', va='center', transform=ax1.transAxes)
            ax1.set_title('NDWI Distribution - No Data')
        
        # Plot 2: NDWI statistics over time
        ax2 = axes[0, 1]
        if self.ndwi is not None:
            time_mean = self.ndwi.mean(dim=['x', 'y'])
            time_std = self.ndwi.std(dim=['x', 'y'])
            dates = pd.to_datetime(time_mean.time.values)
            
            ax2.plot(dates, time_mean, 'o-', color='blue', linewidth=2, markersize=4, label='Mean')
            ax2.fill_between(dates, time_mean - time_std, time_mean + time_std, 
                            alpha=0.3, color='blue', label='±1 Std')
            ax2.set_title('NDWI Temporal Statistics')
            ax2.set_xlabel('Date')
            ax2.set_ylabel('NDWI Value')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)
        else:
            ax2.text(0.5, 0.5, 'No NDWI data available', ha='center', va='center', transform=ax2.transAxes)
            ax2.set_title('NDWI Temporal Statistics - No Data')
        
        # Plot 3: NDWI map (first time step)
        ax3 = axes[1, 0]
        if self.ndwi is not None:
            ndwi_slice = self.ndwi.isel(time=0)
            im3 = ndwi_slice.plot(ax=ax3, cmap='BrBG', vmin=-1, vmax=1, add_colorbar=True)
            ax3.set_title(f'NDWI - {pd.to_datetime(ndwi_slice.time.values).strftime("%Y-%m-%d")}')
        else:
            ax3.text(0.5, 0.5, 'No NDWI data available', ha='center', va='center', transform=ax3.transAxes)
            ax3.set_title('NDWI Map - No Data')
        
        # Plot 4: Water pixels over time
        ax4 = axes[1, 1]
        if self.ndwi is not None:
            water_pixels = []
            dates = []
            
            for t in self.ndwi.time.values:
                try:
                    ndwi_t = self.ndwi.sel(time=t).compute()
                    water_mask = ndwi_t > 0.3
                    count = water_mask.sum().item()
                    water_pixels.append(count)
                    dates.append(pd.to_datetime(t))
                except:
                    continue
            
            ax4.plot(dates, water_pixels, 'o-', color='green', linewidth=2, markersize=4)
            ax4.set_title('Water Pixels Over Time (NDWI > 0.3)')
            ax4.set_xlabel('Date')
            ax4.set_ylabel('Water Pixels')
            ax4.grid(True, alpha=0.3)
            ax4.tick_params(axis='x', rotation=45)
        else:
            ax4.text(0.5, 0.5, 'No NDWI data available', ha='center', va='center', transform=ax4.transAxes)
            ax4.set_title('Water Pixels Over Time - No Data')
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/05_ndwi_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("NDWI analysis saved")


def main():
    """
    Main function demonstrating the WaterAnalyzer usage.
    """
    start_time = time.time()
    logger.info("🚀 Starting Water Analysis Framework")
    logger.info("=" * 50)
    
    # Configuration for single year analysis (2023)
    bbox = [-73.710, 4.600, -73.695, 4.615]  # Small area for testing
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    
    logger.info("⚙️  Configuration:")
    logger.info(f"   📍 Study area: {bbox}")
    logger.info(f"   📅 Time period: {start_date} to {end_date}")
    logger.info(f"   🛰️  Collection: sentinel-2-l2a")
    logger.info(f"   🎨 Bands: B03 (Green), B08 (NIR)")
    logger.info(f"   ☁️  Cloud cover: < 10%")
    logger.info(f"   📊 Max items: 50")
    
    # Initialize analyzer with optimized settings (2 bands for NDWI)
    logger.info("🔧 Initializing WaterAnalyzer with optimized settings...")
    analyzer = WaterAnalyzer(
        collection="sentinel-2-l2a",
        assets=["B03", "B08"],  # Green, NIR only (for NDWI)
        chunksize=4096,  # Larger chunks like notebook
        resolution=100,
        epsg=None,  # Let stackstac handle projection automatically
        enable_parallel=True,  # Enable parallel processing
        n_workers=6  # Use 6 workers for optimal performance
    )
    
    try:
        # Step 1: Search for data
        logger.info("\n" + "="*50)
        logger.info("📡 STEP 1: SEARCHING FOR SATELLITE DATA")
        logger.info("="*50)
        items = analyzer.search_data(
            bbox=bbox,
            start_date=start_date,
            end_date=end_date,
            max_cloud_cover=10.0,  # Stricter cloud cover like notebook
            max_items=50  # Reasonable limit for single year
        )
        
        # Step 2: Load data
        logger.info("\n" + "="*50)
        logger.info("📥 STEP 2: LOADING AND STACKING DATA")
        logger.info("="*50)
        data = analyzer.load_data()
        
        # Step 3: Get statistics
        logger.info("\n" + "="*50)
        logger.info("📊 STEP 3: DATA STATISTICS")
        logger.info("="*50)
        stats = analyzer.get_data_statistics()
        print("\n📈 Data Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Step 4: Calculate water indices
        logger.info("\n" + "="*50)
        logger.info("🌊 STEP 4: CALCULATING WATER INDICES")
        logger.info("="*50)
        indices = analyzer.calculate_water_indices()
        
        # Step 5: Quick NDWI Preview (skip full analysis)
        logger.info("\n" + "="*50)
        logger.info("🔍 STEP 5: QUICK NDWI PREVIEW")
        logger.info("="*50)
        
        # Show NDWI values for each time step
        logger.info("📊 NDWI Values for each image:")
        print(f"\n🌊 NDWI Index Values:")
        print(f"{'Date':<20} {'NDWI Min':<10} {'NDWI Max':<10} {'NDWI Mean':<10}")
        print("-" * 50)
        
        for i, t in enumerate(analyzer.ndwi.time.values):
            try:
                ndwi_slice = analyzer.ndwi.sel(time=t).compute()
                min_val = float(ndwi_slice.min())
                max_val = float(ndwi_slice.max())
                mean_val = float(ndwi_slice.mean())
                date_str = pd.to_datetime(t).strftime("%Y-%m-%d")
                
                print(f"{date_str:<20} {min_val:<10.3f} {max_val:<10.3f} {mean_val:<10.3f}")
                
                # Show first few images only to avoid too much output
                if i >= 5:
                    print(f"... and {len(analyzer.ndwi.time) - 6} more images")
                    break
                    
            except Exception as e:
                print(f"Error processing time step {i}: {e}")
                continue
        
        # Step 6: Simple visualization
        logger.info("\n" + "="*50)
        logger.info("🎨 STEP 6: SIMPLE VISUALIZATION")
        logger.info("="*50)
        
        # Create simple NDWI map for first time step
        logger.info("🗺️  Creating simple NDWI map...")
        analyzer.plot_water_index_map(
            water_index="ndwi", 
            time_index=0, 
            save_path="images/quick_ndwi_map.png"
        )
        logger.info("✅ Quick NDWI map saved to images/quick_ndwi_map.png")
        
        # Save NDWI data for further analysis
        logger.info("💾 Saving NDWI data for further analysis...")
        analyzer.save_ndwi_data("ndwi_data_2023.npz")
        logger.info("✅ NDWI data saved to ndwi_data_2023.npz")
        
        # Summary
        logger.info("\n" + "="*50)
        logger.info("🎉 QUICK NDWI ANALYSIS COMPLETED!")
        logger.info("="*50)
        # Performance summary
        end_time = time.time()
        print_performance_summary(start_time, end_time, data.shape)
        
        logger.info("📁 Generated files:")
        logger.info("   📁 images/quick_ndwi_map.png")
        logger.info("   💾 ndwi_data_2023.npz (NDWI data for further analysis)")
        logger.info("   📄 water_analysis.log")
        logger.info("📊 NDWI values shown above for each image")
        
        return analyzer, None
        
    except Exception as e:
        logger.error(f"❌ Error in main analysis: {e}")
        logger.error("🔍 Check the log file for detailed error information")
        raise
    
    finally:
        # Clean up Dask client
        if 'analyzer' in locals():
            analyzer.stop_dask_client()
            logger.info("🧹 Cleanup completed")


if __name__ == "__main__":
    # Run the analysis
    print("🚀 Starting Water Analysis Framework...")
    print("📝 Detailed logs will be saved to 'water_analysis.log'")
    print("🖼️  Images will be saved to 'images/' folder")
    print("=" * 60)
    
    try:
        analyzer, results = main()
        
        print("\n" + "=" * 60)
        print("🎉 QUICK NDWI ANALYSIS COMPLETED!")
        print("=" * 60)
        print("📁 Generated files:")
        print("   📁 images/quick_ndwi_map.png - NDWI map for first image")
        print("   💾 ndwi_data_2023.npz - NDWI data for further analysis")
        print("   📄 water_analysis.log - Detailed execution log")
        print("\n💡 NDWI values for each image are shown above!")
        print("💡 You can load the .npz file with: data = np.load('ndwi_data_2023.npz')")
        
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        print("🔍 Check the log file for detailed error information")
        raise 