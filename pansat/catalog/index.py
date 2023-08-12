#!/usr/bin/env python3
"""
pansat.catalog.index
====================

The ``pansat.catalog.index`` implements indices, which keep track of
the temporal and spatial coverage of data files from a single product.
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager, TimeoutError
from pathlib import Path
import queue

import numpy as np
import xarray as xr
import geopandas
import rich.progress

from pansat.time import TimeRange, to_datetime64
from pansat.file_record import FileRecord
from pansat.granule import Granule, merge_granules
from pansat.products import Product, GranuleProduct, get_product
from pansat.geometry import ShapelyGeometry


def _get_index_data(product, path):
    """
    Extracts index data from a single path.

    Args:
        product: A 'pansat.Product' object representing the data product
            that is being indexed.
        path: A path pointing to a data file of that product.

    Return:
        A tuple ``(start_time, end_time, local_path, geom)`` containing
        the start and end time, the path of the file as sting and
        ``geom`` a geometry representing the spatial coverage of the file.
    """
    if product.matches(path.name) is None:
        return []

    rec = FileRecord(path)

    if isinstance(product, GranuleProduct):
        return product.get_granules(rec)

    start_time, end_time = product.get_temporal_coverage(rec)
    geom = product.get_spatial_coverage(rec)
    local_path = str(path)

    return [Granule(rec, TimeRange(start_time, end_time), geom)]


def _pandas_to_granule(
        product,
        data
):
    """
    Converts a pandas dataframe of file indices into a list
    of file records.

    Args:
        product: The 'pansat.Product' object representing the
            product.
        data: A pandas dataframe containing a selection of
            data file indices.

    Return:
        A list of file records pointing to the files in 'data'.
    """
    granules = []

    if "local_path" in data.columns:
        for row in data.itertuples():
            local_path = row.local_path
            rec = FileRecord(local_path, product)
            start_time = row.start_time
            end_time = row.end_time
            primary_index_name = row.primary_index_name
            primary_index_start = row.primary_index_start
            primary_index_end = row.primary_index_end
            secondary_index_name = row.secondary_index_name
            secondary_index_start = row.secondary_index_start
            secondary_index_end = row.secondary_index_end
            geo = ShapelyGeometry(row.geometry)
            granules.append(Granule(
                rec,
                TimeRange(start_time, end_time),
                geo,
                primary_index_name,
                (primary_index_start, primary_index_end),
                secondary_index_name,
                (secondary_index_start, secondary_index_end),
            ))
    elif "remote_path" in data.columns:
        for row in data.itertuples():
            remote_path = row.remote_path
            filename = row.filename
            rec = FileRecord.from_remote(
                product,
                None,
                remote_path,
                filename
            )
            start_time = row.start_time
            end_time = row.end_time
            primary_index_name = row.primary_index_name
            primary_index_start = row.primary_index_start
            primary_index_end = row.primary_index_end
            secondary_index_name = row.secondary_index_name
            secondary_index_start = row.secondary_index_start
            secondary_index_end = row.secondary_index_end
            geo = ShapelyGeometry(row.geometry)
            granules.append(Granule(
                rec,
                TimeRange(start_time, end_time),
                geo,
                primary_index_name,
                (primary_index_start, primary_index_end),
                secondary_index_name,
                (secondary_index_start, secondary_index_end),
            ))
    else:
        raise ValueError(
            "Index data must provide either a 'local_path' column or "
            " a 'remote_path' column."
        )
    return granules


class Index:
    """
    A index keeps track of data files of specific product.


    Attributes:
        data: A 'geopandas.Dataframe' containing coverage information
            of all indexed files.
    """
    @classmethod
    def load(cls, path):
        """
        Load an index.

        Args:
            path: Path to an Apache parquet file containing the index.

        Return:
            The loaded index.
        """
        path = Path(path)
        data = geopandas.read_parquet(path)
        product_name = path.stem
        product = get_product(product_name)
        return cls(product, data)


    @classmethod
    def index(cls, product, files, n_processes=None):
        """
        Index data files.

        Args:
            product: The pansat product to index.
            files: A list of Path objects pointing to the data files to
                index.
            n_processes: The number of processes to use for parallel
                processing. If 'None', the indexing will not be performed
                in parallel.

        Return:
           An Index object containing an index of all files.
        """
        geoms = []
        start_times = []
        end_times = []
        local_paths = []
        primary_index_name = []
        primary_index_start = []
        primary_index_end = []
        secondary_index_name = []
        secondary_index_start = []
        secondary_index_end = []

        if n_processes is None:
            for path in files:

                index_data = _get_index_data(product, path)

                for granule in index_data:
                    start_times.append(granule.time_range.start)
                    end_times.append(granule.time_range.end)
                    local_paths.append(str(granule.file_record.local_path))
                    primary_index_name.append(granule.primary_index_name)
                    primary_index_start.append(granule.primary_index_range[0])
                    primary_index_end.append(granule.primary_index_range[1])
                    secondary_index_name.append(granule.secondary_index_name)
                    secondary_index_start.append(granule.secondary_index_range[0])
                    secondary_index_end.append(granule.secondary_index_range[1])
                    geoms.append(granule.geometry.to_shapely())
        else:
            pool = ProcessPoolExecutor(
                max_workers=n_processes
            )
            tasks = []
            for path in files:
                tasks.append(pool.submit(_get_index_data, product, path))

            n_tasks = len(tasks)

            with rich.progress.Progress() as prog:
                prog_task = prog.add_task(
                    description="Indexing files: ",
                    start=True,
                    total=n_tasks
                )

                for ind, task in enumerate(as_completed(tasks)):

                    index_data = task.result()
                    prog.update(prog_task, advance=1)
                    if index_data is None:
                        continue

                    for granule in index_data:
                        start_times.append(granule.time_range.start)
                        end_times.append(granule.time_range.end)
                        local_paths.append(str(granule.file_record.local_path))
                        primary_index_name.append(granule.primary_index_name)
                        primary_index_start.append(granule.primary_index_range[0])
                        primary_index_end.append(granule.primary_index_range[1])
                        secondary_index_name.append(granule.secondary_index_name)
                        secondary_index_start.append(granule.secondary_index_range[0])
                        secondary_index_end.append(granule.secondary_index_range[1])
                        geoms.append(granule.geometry.to_shapely())

                prog.refresh()

        data = geopandas.GeoDataFrame(
            data={
                "start_time": start_times,
                "end_time": end_times,
                "local_path": local_paths,
                "primary_index_name": primary_index_name,
                "primary_index_start": primary_index_start,
                "primary_index_end": primary_index_end,
                "secondary_index_name": secondary_index_name,
                "secondary_index_start": secondary_index_start,
                "secondary_index_end": secondary_index_end,
            },
            geometry=geoms
        )
        data.sort_values("start_time", inplace=True)
        return cls(product, data)

    def __init__(self, product, data):
        """
        Args:
            product: The pansat product whose data files are indexed by
                this index.
            data: A geopandas.GeoDataFrame containing the start and end time
                and geographical coverage of all data files.
        """
        self.product = product
        self.data = data


    def __add__(self, other):
        """Merge two indices."""
        if not self.product == other.product:
            raise ValueError(
                "Combining to Index object requires them to refer to the"
                " same product."
            )
        product = self.product
        data = pd.merge([self.data, other.data], how="outer")
        return Index(product, data)

    def __repr__(self):
        return (
            f"<Index of '{self.product.name}' containing "
            f"{self.data.start_time.size} entries>"
        )


    def find(self, time_range=None, roi=None):
        """
        Find entries in Index within given time range and location.
        """
        if time_range is None and roi is None:
            return _pandas_to_granule(
                self.product,
                self.data
            )

        if time_range is None:
            selected = self.data
        else:
            if not isinstance(time_range, TimeRange):
                time_range = TimeRange(time_range, time_range)
            selected = self.data.loc[
                ~((self.data.start_time > time_range.end) |
                  (self.data.end_time < time_range.start))
            ]

        if roi is None:
            return _pandas_to_granule(
                self.product,
                selected
            )

        roi = roi.to_shapely()
        indices = selected.intersects(roi)

        return _pandas_to_granule(
            self.product,
            selected.loc[indices]
        )

    def save(self, path):
        """
        Save an index.

        Args:
            path: Path to a directory to which to save the index.

        Return:
            A 'Path' object pointing to the saved index.
        """
        path = Path(path)
        if not path.is_dir():
            raise ValueError(
                "'path' must point to a directory."
            )
        output_file = path / f"{self.product.name}.idx"
        data = self.data.to_parquet(output_file)
        return output_file

    def search_interactive(self):
        from pansat.catalog.interactive import visualize_index
        return visualize_index(self)


def merge_matches(match_1, match_2):
    """
    Merge two matches if they are adjacent.

    Args:
        match_1: Tuple containing the first match.
        match_2: Tuple containing the second match.

    Return:
        A list that either contains the merged match or the two matches if they are not adjacent.
    """
    if match_1[0].is_adjacent(match_2[0]):
        if match_1[1].is_adjacent(match_2[1]):
            return [(
                match_1[0].merge(match_2[0]),
                match_1[1].merge(match_2[1]),
            )]
    return [match_1, match_2]


def _find_matches_rec(
        prod_l,
        index_data_l,
        prod_r,
        index_data_r,
        time_diff,
        merge=True,
        done_queue=None
):
    """
    Recursively search for matching granules in two given index
    data frames.

    Args:
        index_data_l: A geopandas dataframe contraining the granules
            of the first product.
        index_data_r: A geopandas dataframe containing the granules of of the second product.
        timedeiff: A numpy.timedelta64 object specifying the maximum
            time difference between two observations for them to be
            considered a match.
        merge: Whether matches of adjacent granules should be merged.
        done_queue: Optional queue used to communicate progress from
            multiple processes.

    Return:
        A list of tuples ``(granule_l, granules_r)`` mapping a granule
        from the first index to a list of overlapping granules from
        the second index.
    """

    if index_data_r.shape[0] == 0:
        if done_queue is not None:
            done_queue.put(index_data_l.shape[0])
        return []

    # If we have more than 2 granules in left block,
    # split.
    if index_data_l.shape[0] > 1:

        n_l = index_data_l.shape[0]

        split_1_l = index_data_l.iloc[:n_l // 2]
        split_2_l = index_data_l.iloc[n_l // 2:]

        split_1_start = split_1_l.start_time.iloc[0] - time_diff
        split_1_end = split_1_l.end_time.iloc[-1] + time_diff
        inds_1_r = (
            (index_data_r.end_time > split_1_start) *
            (index_data_r.start_time < split_1_end)
        )
        split_1_r = index_data_r.loc[inds_1_r]

        split_2_start = split_2_l.start_time.iloc[0] - time_diff
        split_2_end = split_2_l.end_time.iloc[-1] + time_diff
        inds_2_r = (
            (index_data_r.end_time > split_2_start) *
            (index_data_r.start_time < split_2_end)
        )
        split_2_r = index_data_r.loc[inds_2_r]

        matches_1 = _find_matches_rec(
            prod_l,
            split_1_l,
            prod_r,
            split_1_r,
            time_diff,
            merge=merge,
            done_queue=done_queue
        )
        matches_2 = _find_matches_rec(
            prod_l,
            split_2_l,
            prod_r,
            split_2_r,
            time_diff,
            merge=merge,
            done_queue=done_queue
        )

        if len(matches_1) == 0:
            return matches_2

        if len(matches_2) == 0:
            return matches_1

        if merge:
            return (
                matches_1[:-1] +
                merge_matches(matches_1[-1], matches_2[0]) +
                matches_2[1:]
            )
        return matches_1 + matches_2

    start_time = index_data_l.start_time.iloc[0] - time_diff
    end_time = index_data_l.end_time.iloc[0] + time_diff

    matches = (
        (index_data_r.end_time > start_time) *
        (index_data_r.start_time < end_time)
    )
    selected = index_data_r.loc[matches]
    matches = selected.intersects(index_data_l.geometry.iloc[0])
    granules_r = _pandas_to_granule(prod_r, selected.loc[matches])

    if len(granules_r) == 0:
        if done_queue is not None:
            done_queue.put(1)
        return []

    if merge:
        granules_r = merge_granules(granules_r)

    granule_l = _pandas_to_granule(prod_l, index_data_l)[0]

    if done_queue is not None:
        done_queue.put(1)
    return [(granule_l, granule) for granule in granules_r]


def find_matches(
        index_l,
        index_r,
        time_diff=None,
        n_processes=None,
        merge=True
):
    """
    Find matching observations between two indices.

    Args:
        index_l: The index containing the granules of the first product.
        index_r: The index containing the granules of the second product.
        time_diff: The maximum time difference between two observations
             for two observations to be considered a match.
        merge_matches: If 'True', adjacent granules will be merged in
            with the aim of combining matches extending over several
            granules into one.

    Return:
        A list of tuples ``(granule_l, granules_r)`` mapping a granule
        from the first index to a list of overlapping granules from
        the second index.
    """

    if time_diff is None:
        time_diff = np.timedelta64("5", "m")

    if n_processes is None:
        return _find_matches_rec(
            index_l.product,
            index_l.data,
            index_r.product,
            index_r.data,
            time_diff=time_diff,
            merge=merge
        )

    pool = ProcessPoolExecutor(max_workers=n_processes)

    n_granules_l = index_l.data.shape[0]
    granules_per_proc = n_granules_l // n_processes
    rem = n_granules_l % n_processes

    manager = Manager()
    done_queue = manager.Queue()

    ind_start = 0
    tasks = []

    for i in range(n_processes):

        ind_start = i * granules_per_proc + min(i, rem)
        ind_end = ind_start + granules_per_proc
        if i < rem:
            ind_end += 1

        index_data_l = index_l.data.iloc[ind_start:ind_end]
        start_time = index_data_l.start_time.iloc[0] - time_diff
        end_time = index_data_l.end_time.iloc[-1] + time_diff
        inds_r = (
            (index_r.data.end_time > start_time) *
            (index_r.data.start_time < end_time)
        )
        index_data_r = index_r.data.loc[inds_r]

        tasks.append(pool.submit(
            _find_matches_rec,
            index_l.product,
            index_data_l,
            index_r.product,
            index_data_r,
            time_diff=time_diff,
            merge=merge,
            done_queue=done_queue,
        ))

    assert ind_end == n_granules_l

    matches = []
    with rich.progress.Progress() as prog:
        prog_task = prog.add_task(
            description="Matching indices: ",
            start=True,
            total=n_granules_l
        )

        for task in tasks:
            while not task.done():
                try:
                    elems = done_queue.get(timeout=1)
                    prog.update(prog_task, advance=elems)
                except (TimeoutError, queue.Empty):
                    pass

            matches_t = task.result()

            if len(matches_t) == 0:
                continue

            if len(matches) == 0:
                matches = matches_t
                continue

            if merge:
                matches = (
                    matches[:-1] +
                    merge_matches(matches[-1], matches_t[0]) +
                    matches_t[1:]
                )
            else:
                matches += matches_t

        # Finish progress bar.
        while done_queue.qsize():
            elems = done_queue.get()
            prog.update(prog_task, advance=elems)

    return matches



def matches_to_geopandas(matches):
    """
    Convert a list of matches to geopandas data frames.

    Args:
        matches: A list of match tuples such as the one returned by the
            'find_matches' function.

    Return:
        A tuple ``(dframe_l, dframe_r)`` containing two dataframes, each
        containing the matches from the two respective datasets.
    """
    dframes = []
    for ind in range(2):

        match_indices = []
        geoms = []
        start_times = []
        end_times = []
        local_paths = []
        filenames = []
        remote_paths = []
        primary_index_name = []
        primary_index_start = []
        primary_index_end = []
        secondary_index_name = []
        secondary_index_start = []
        secondary_index_end = []

        for match_ind, match in enumerate(matches):

            granule = match[ind]

            match_indices.append(match_ind)
            geoms.append(granule.geometry.to_shapely())
            start_times.append(granule.time_range.start)
            end_times.append(granule.time_range.end)

            if granule.file_record.local_path is not None:
                local_paths.append(str(granule.file_record.local_path))
            else:
                local_paths.append("")

            if granule.file_record.remote_path is not None:
                remote_paths.append(str(granule.file_record.remote_path))
            else:
                remote_paths.append("")
            filenames.append(str(granule.file_record.filename))

            primary_index_name.append(granule.primary_index_name)
            primary_index_start.append(granule.primary_index_range[0])
            primary_index_end.append(granule.primary_index_range[1])
            secondary_index_name.append(granule.secondary_index_name)
            secondary_index_start.append(granule.secondary_index_range[0])
            secondary_index_end.append(granule.secondary_index_range[1])

        dframes.append(
            geopandas.GeoDataFrame(
                data={
                    "start_time": start_times,
                    "end_time": end_times,
                    "local_path": local_paths,
                    "remote_path": remote_paths,
                    "filename": filenames,
                    "primary_index_name": primary_index_name,
                    "primary_index_start": primary_index_start,
                    "primary_index_end": primary_index_end,
                    "secondary_index_name": secondary_index_name,
                    "secondary_index_start": secondary_index_start,
                    "secondary_index_end": secondary_index_end,
                },
                geometry=geoms,
                index=match_indices
            )
        )
    return tuple(dframes)
