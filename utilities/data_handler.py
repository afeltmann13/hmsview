import geopandas as gpd
import requests as req
import tempfile as tmp
import datetime as dt
import os


from zipfile import ZipFile
from dataclasses import dataclass

@dataclass
class hms_data_handler:
    """Class to handle data operations for the HMS project."""
   
    start_date: dt.datetime = dt.date.today() - dt.timedelta(days=1)
    date_delta: int = 7

    def __post_init__(self):
        rolling_days = [self.start_date - dt.timedelta(days = x) for x in range(self.date_delta)]
        state_data = self.__open_zipped_data__("https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip").to_crs("EPSG:4326").dissolve()
        self.smoke_url_list = [[x,f"https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile/{x.strftime('%Y')}/{x.strftime('%m')}/hms_smoke{x.strftime('%Y%m%d')}.zip"] for x  in rolling_days]
        self.smoke_data =  [[x[0],self.__open_zipped_data__(x[1]).to_crs("EPSG:4326").clip(state_data).dissolve(by=['Density'])] for x in self.smoke_url_list]
    
    @staticmethod
    def __open_zipped_data__(url) -> gpd.GeoDataFrame:
        """
        Args:
            data (list of tuples): Each tuple contains state, county, and URL to the zipped data.
        """
        with req.get(url, timeout=500) as r:
            r.raise_for_status()
            with tmp.TemporaryFile() as tmp_file:    
                tmp_file.write(r.content)
                with tmp.TemporaryDirectory() as tmp_dir:          
                    with ZipFile(tmp_file, "r") as zip_file:
                        zip_file.extractall(tmp_dir)
                    for file in os.listdir(tmp_dir):
                        if file.endswith(".shp"):
                            file_location = os.path.join(tmp_dir, file)
                            downloaded_data = gpd.read_file(file_location).to_crs("EPSG:4326")
                        
        return downloaded_data