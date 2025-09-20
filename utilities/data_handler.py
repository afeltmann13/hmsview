import geopandas as gpd
import requests as req
import tempfile as tmp
import datetime as dt
import os


from zipfile import ZipFile
from dataclasses import dataclass


@dataclass
class HmsDataHandler:
    """Class to handle data operations for the HMS project."""

    start_date: dt.datetime = dt.date.today() - dt.timedelta(days=1)
    year_delta: float = 4
    smoke_base_url: str = "https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile/"
    fire_base_url: str = "https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Fire_Points/Shapefile/"

    def __post_init__(self):
        """Initializes the hms_data_handler with the necessary data."""
        self.rolling_days = [
            self.start_date - dt.timedelta(days=x) for x in range(int(self.year_delta*365))
        ]
        self.state_data = (
            self.__open_data__(
                "https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip"
            ).to_crs("EPSG:4326").dissolve()
        )

    @staticmethod
    def __open_data__(url):
        """
        Args:
            data (list of tuples): Each tuple contains state, county, and URL to the zipped data.
        """
        
        if os.path.exists(url):
            with tmp.TemporaryFile() as tmp_file:
                    tmp_file.write(url)
                    with tmp.TemporaryDirectory() as tmp_dir:
                        with ZipFile(tmp_file, "r") as zip_file:
                            zip_file.extractall(tmp_dir)
                        for file in os.listdir(tmp_dir):
                            if file.endswith(".shp"):
                                file_location = os.path.join(tmp_dir, file)
                                return gpd.read_file(file_location).to_crs("EPSG:4326")
        else:
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
                                return gpd.read_file(file_location).to_crs("EPSG:4326")

    @staticmethod
    def smoke_style_row(row):
        """
        Args:
            row (pandas.Series): A row from the GeoDataFrame containing smoke data.
            Returns:
                dict: A dictionary with style properties for the smoke data."""

        if row["Density"] == "Light":
            return {"fillColor": "#b5b5b5", "weight": 1, "color": "#b5b5b5"}
        elif row["Density"] == "Medium":
            return {"fillColor": "#6b6b6b", "weight": 1, "color": "#6b6b6b"}
        elif row["Density"] == "Heavy":
            return {"fillColor": "#AC0000", "weight": 1, "color": "#AC0000"}
        else:
            return {"fillColor": "#0201015A", "weight": 1, "color": "#0201015A"}
 
    def get_data_links(self):
        
        days_back = self.rolling_days
        smoke_url= self.smoke_base_url
        fire_url = self.fire_base_url
        
        smoke_url_list = [
            [
                x,
                f"{smoke_url}{x.strftime('%Y')}/{x.strftime('%m')}/hms_smoke{x.strftime('%Y%m%d')}.zip",
            ]
            for x in days_back
        ]
        fire_url_list = [
            [
                x,
                f"{fire_url}/{x.strftime('%Y')}/{x.strftime('%m')}/hms_fire{x.strftime('%Y%m%d')}.zip",
            ]
            for x in days_back
        ]

        return smoke_url_list, fire_url_list

    def get_smoke_data(self):
        """_summary_

        Returns:
            list: _description_
        -----------
        Retrieves smoke data for the specified date range, processes it, and returns a list of GeoDataFrames.
        -----------
        This method fetches smoke data from the HMS service, processes it to the correct coordinate reference system (CRS),
        clips it to the state boundaries, and applies a style to each row based on the smoke density.
        The processed data is returned as a list of GeoDataFrames, each containing the date and styled smoke data.
        """
        
        smoke_list, _ = self.get_data_links()
        
        smoke_data_raw = [
            [
                x[0],
                self.__open_data__(x[1])
                .to_crs("EPSG:4326")
                .clip(self.state_data)
                .dissolve(by=["Density"], as_index=False),
            ]
            for x in smoke_list
        ]
        return [
            [
                x[0],
                x[1].assign(style=x[1].apply(self.smoke_style_row, axis=1))
                ]
            for x in smoke_data_raw
        ]

    def get_fire_data(self):
        
        _, fire_list = self.get_data_links()
        
        return [
            [
                x[0],
                self.__open_data__(x[1])
                .to_crs("EPSG:4326")
                .clip(self.state_data)
            ]
            for x in fire_list
        ]
