"""
pansat.catalog.interactive
==========================

This module provides functions to interactively visualize catalogs
and indices.
"""

try:
    from ipyleaflet import Map, WKTLayer, DrawControl
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "This ipyleaflet module is required for interactively visualizing" "indices."
    )
from ipywidgets import HBox, VBox, DatePicker, Button, HTML, Text, Label
from IPython.display import display

import numpy as np
import pandas as pd


from pansat.catalog.index import _pandas_to_granule
from pansat.geometry import MultiPolygon
from pansat.time import TimeRange


class GranuleDrawer:
    """
    Helper class that manages the drawing of granules onto the
    leaflet widget.
    """

    def __init__(self, leaflet_map, max_granules=500):
        """
        Args:
            leaflet_map: The leaflet widget onto which to draw the granules.
            max_granules: The maximum number of granules to draw onto the
                map. Limiting the number of granules to draw is required to
                avoid freezing of the map.
        """
        self.leaflet_map = leaflet_map
        self._granules = set()
        self.layers = {}
        self.max_granules = max_granules
        self.base_layer = leaflet_map.layers[0]

    @property
    def granules(self):
        """
        A set containing all granules that are currently drawn on the map.
        """
        return self._granules

    @granules.setter
    def granules(self, new_granules):
        """
        This setter function updates the drawn granules so that all
        granules in 'new_granules' are drawn without exceeding the
        limit set by 'self.max_granules'.

        Args:
            new_granules: An iterable of granules to drawn.
        """
        new_granules = set(new_granules)

        old_granules = self._granules

        remove = old_granules - new_granules
        for granule in remove:
            layer = self.layers.pop(granule)
            self.leaflet_map.remove_layer(layer)

        add = new_granules - old_granules
        added = []

        for granule in add:
            if len(self.layers) > self.max_granules:
                break

            info_str = rf"""
            <b>Filename:  </b> {granule.file_record.filename}<br>
            <b>Start time:</b> {granule.time_range.start}<br>
            <b>End time:</b> {granule.time_range.end}<br>
            """

            if granule.primary_index_name != "":
                info_str += f"""
                <b>{granule.primary_index_name}</b>: {granule.primary_index_range}
                """

            layer = WKTLayer(
                wkt_string=granule.geometry.to_shapely().wkt, popup=HTML(info_str)
            )
            self.leaflet_map.add_layer(layer)
            self.layers[granule] = layer
            added.append(granule)

        self._granules = new_granules & old_granules | set(added)

    def clear(self):
        self.leaflet_map.clear_layers()
        self.leaflet_map.add_layer(self.base_layer)
        self.layers = {}
        self._granules = set()


class PolygonManager:
    """
    This class manages the region polygon that can be used to limit
    the search regions.
    """

    def __init__(self, leaflet_map):
        """
        Args:
            leaflet_map: The leaflet widget used to display the data.
        """
        self.leaflet_map = leaflet_map
        self._polygon = None
        self._layer = None

    @property
    def polygon(self):
        """
        A 'pansat.geometry.MultiPolygon' representing the shape drawn
        on the map.
        """
        return self._polygon

    @polygon.setter
    def polygon(self, coords):
        new_polygon = MultiPolygon(coords)
        if self._layer is not None:
            self.leaflet_map.remove_layer(self._layer)
        self._layer = WKTLayer(
            wkt_string=new_polygon.to_shapely().wkt, style={"color": "red"}
        )
        self.leaflet_map.add_layer(self._layer)
        self._polygon = new_polygon

    def reset(self):
        if self._layer is not None:
            self.leaflet_map.add_layer(self._layer)


class SearchResult:
    """
    A container class the holds the search results from the
    interactive search.
    """

    def __init__(self):
        self._granules = set()

    @property
    def granules(self):
        """
        The granules that were the result of the search.
        """
        return self._granules

    @granules.setter
    def granules(self, new_granules):
        self._granules = new_granules


def visualize_index(index, max_granules=10):
    granules = _pandas_to_granule(index.product, index.data.iloc[:max_granules])
    start_time = pd.to_datetime(index.data.start_time[0])
    end_time = pd.to_datetime(index.data.start_time[max_granules - 1])

    ll_map = Map(zoom=1)
    draw_control = DrawControl()
    draw_control.polygon = {
        "shapeOptions": {
            "color": "#6be5c3",
        },
        "drawError": {"color": "#dd253b", "message": "Oups!"},
        "allowIntersection": False,
    }
    draw_control.polyline = {}
    draw_control.circlemarker = {}

    poly_manager = PolygonManager(ll_map)

    def remove_polygons(*args, **kwargs):
        poly_manager.polygon = kwargs["geo_json"]["geometry"]["coordinates"]
        draw_control.clear_polygons()

    draw_control.on_draw(remove_polygons)
    ll_map.add_control(draw_control)

    date_field_1 = Text(
        description="Start time: ", value=start_time.strftime("%Y-%m-%dT%H:%M:%S")
    )
    date_field_2 = Text(
        description="End time: ", value=end_time.strftime("%Y-%m-%dT%H:%M:%S")
    )
    label = Label()

    button = Button(description="Search")

    granule_drawer = GranuleDrawer(ll_map)
    granule_drawer.granules = granules

    first_row = HBox([date_field_1, date_field_2, button, label])
    box = VBox([first_row, ll_map])

    results = SearchResult()

    def search(button):
        try:
            date_1 = np.datetime64(date_field_1.value)
        except ValueError:
            label.value = "Please provide a valid start date!"
            return None

        try:
            date_2 = np.datetime64(date_field_2.value)
        except ValueError:
            label.value = "Please provide a valid end date!"
            return None

        granule_drawer.clear()
        roi = poly_manager.polygon

        label.value = ""

        time_range = TimeRange(date_1, date_2)
        granules = index.find(time_range=time_range, roi=roi)
        results.granules = granules
        granule_drawer.granules = granules
        poly_manager.reset()

    button.on_click(search)
    display(box)

    return results
