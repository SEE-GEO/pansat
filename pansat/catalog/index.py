"""
pansat.catalog.index
====================

The ``pansat.catalog.index`` implements indices, which keep track of
the temporal and spatial coverage of data files from a single product.
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
from multiprocessing import Manager, TimeoutError
from pathlib import Path
import queue
from typing import Dict, List, Optional, Tuple, Set, Union
import logging

from filelock import FileLock
import numpy as np
import xarray as xr
import geopandas
import pandas as pd
import rich.progress

from pansat.time import TimeRange, to_datetime64
from pansat.file_record import FileRecord
from pansat.granule import Granule, merge_granules
from pansat.products import Product, GranuleProduct, get_product
from pansat.geometry import Geometry, ShapelyGeometry
from pansat import database


LOGGER = logging.getLogger(__file__)


def find_pansat_catalog(path):
    """
    Walks down the directory tree starting from a given path and looks
    for '.pansat' folders that may a catalog.

    Args:
        path: Path object pointing to a directory at which to start searching
            for .pansat folders.

    Return:
        A Catalog object if a '.pansat' folder is found. 'None' otherwise.
    """
    curr_path = path.absolute()
    while curr_path != curr_path.parent:
        pansat_path = curr_path / ".pansat"
        if pansat_path.exists() and pansat_path.is_dir():
            return Catalog(pansat_path)
    return None


def _get_index_data(product: Product, rec: Union[FileRecord, Path]) -> List[Granule]:
    """
    Extracts index data from a product file.

    Args:
        product: A 'pansat.Product' object representing the data product
            that is being indexed.
        rec: A file record identifying a given product file.

    Return:
        A tuple ``(start_time, end_time, local_path, geom)`` containing
        the start and end time, the path of the file as sting and
        ``geom`` a geometry representing the spatial coverage of the file.
    """
    if not isinstance(rec, FileRecord):
        rec = FileRecord(rec)

    if product.matches(rec) is None:
        logging.info("Filename does not match regular expression.")
        return []

    if isinstance(product, GranuleProduct):
        return product.get_granules(rec)

    start_time, end_time = product.get_temporal_coverage(rec)
    geom = product.get_spatial_coverage(rec)

    return [Granule(rec, TimeRange(start_time, end_time), geom)]


def _dataframe_to_granules(
    product: Product, data: geopandas.GeoDataFrame
) -> List[Granule]:
    """
    Converts a pandas dataframe of file indices into a list
    of file records.

    Args:
        product: The 'pansat.Product' object representing the
            product.
        data: A pandas dataframe containing a selection of
            data file indices.

    Return:
        A list granules objects representing the granules in 'data'.
    """
    granules = []
    for row in data.itertuples():
        local_path = None
        if "local_path" in data.columns and row.local_path != "None":
            local_path = row.local_path
        remote_path = None
        if "remote_path" in data.columns and row.remote_path != "None":
            remote_path = row.remote_path
        filename = row.filename
        rec = FileRecord(
            local_path,
            product=product,
            remote_path=remote_path,
            filename=filename
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
        granules.append(
            Granule(
                rec,
                TimeRange(start_time, end_time),
                geo,
                primary_index_name,
                (primary_index_start, primary_index_end),
                secondary_index_name,
                (secondary_index_start, secondary_index_end),
            )
        )
    return granules


def _granules_to_dataframe(granules: List[Granule]) -> geopandas.GeoDataFrame:
    """
    Coverts a list of granules into a pandas Dataframe.

    Args:
        granules: A list of granules to parse into a dataframe.

    Return:
        A pandast dataframe containing representations of the given
        granules.
    """
    geoms = []
    start_times = []
    end_times = []
    local_paths = []
    remote_paths = []
    filenames = []
    primary_index_name = []
    primary_index_start = []
    primary_index_end = []
    secondary_index_name = []
    secondary_index_start = []
    secondary_index_end = []

    for granule in granules:
        start_times.append(granule.time_range.start)
        end_times.append(granule.time_range.end)
        local_paths.append(str(granule.file_record.local_path))
        remote_paths.append(str(granule.file_record.remote_path))
        filenames.append(str(granule.file_record.filename))
        primary_index_name.append(granule.primary_index_name)
        primary_index_start.append(granule.primary_index_range[0])
        primary_index_end.append(granule.primary_index_range[1])
        secondary_index_name.append(granule.secondary_index_name)
        secondary_index_start.append(granule.secondary_index_range[0])
        secondary_index_end.append(granule.secondary_index_range[1])
        geoms.append(granule.geometry.to_shapely())

    data = geopandas.GeoDataFrame(
        data={
            "start_time": start_times,
            "end_time": end_times,
            "local_path": pd.Series(local_paths, dtype="string"),
            "remote_path": pd.Series(remote_paths, dtype="string"),
            "filename": pd.Series(filenames, dtype="string"),
            "primary_index_name": primary_index_name,
            "primary_index_start": primary_index_start,
            "primary_index_end": primary_index_end,
            "secondary_index_name": secondary_index_name,
            "secondary_index_start": secondary_index_start,
            "secondary_index_end": secondary_index_end,
        },
        geometry=geoms,
    )
    data.sort_values("start_time", inplace=True)
    return data


class Index:
    """
    A index keeps track of data files of specific product.


    Attributes:
        data: A 'geopandas.Dataframe' containing coverage information
            of all indexed files.
    """
    @classmethod
    def load(
            cls,
            product: Product,
            db_path: Path,
            time_range: Optional[TimeRange] = None
    ) -> "Index":
        """
        Load an index.

        Args:
            product: The pansat product whose index to load from the databse.
            db_path: Path to a pansat database containing a table with entries
                for this index.
            time_range: Optional time range object to restrict the granules
                to load.

        Return:
            The loaded index.

        Raises:
            'ValueError' if the product corresponding to the index could
            not be found.
        """
        tables = database.get_table_names(db_path)
        if product.name not in tables:
            return cls(product, None)

        data =  database.load_index_data(
            product,
            db_path,
            time_range=time_range
        )
        return cls(product, data)

    @classmethod
    def load_indices(
            cls,
            db_path,
            time_range: Optional[TimeRange] = None
    ) -> Dict[str, "Index"]:
        """
        Load all indices in a database.

        Args:
            db_path: Path to a pansat database containing a table with entries
                for this index.
            time_range: Optional time range object to restrict the granules
                to load.

        Return:
            A dictionary mapping product names to indices.
        """
        table_names = database.get_table_names(db_path)
        indices = {}
        for table_name in table_names:
            try:
                product = get_product(table_name)
            except ValueError:
                LOGGER.warning(
                    "Found table named '%s' in database but could not load find "
                    "a corresponding pansat product."
                )
                continue
            data = database.load_index_data(
                product,
                db_path,
                time_range=time_range
            )
            indices[product.name] = cls(product, data)
        return indices

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
        dframes = []
        granules = []

        if n_processes is None:
            for rec in files:
                try:
                    granules += _get_index_data(product, rec)
                except Exception as exc:
                    LOGGER.warning(
                        "Indexing of file record '%s' frailed with the "
                        "following error:\n %s",
                        rec,
                        exc
                    )
        else:
            pool = ProcessPoolExecutor(max_workers=n_processes)
            tasks = []
            for rec in files:
                tasks.append(pool.submit(_get_index_data, product, rec))

            n_tasks = len(tasks)

            with rich.progress.Progress() as prog:
                prog_task = prog.add_task(
                    description="Indexing files: ", start=True, total=n_tasks
                )

                for ind, task in enumerate(as_completed(tasks)):
                    granules_t = task.result()
                    prog.update(prog_task, advance=1)
                    if granules_t is None:
                        continue
                    granules += granules_t

                prog.refresh()

        data = _granules_to_dataframe(granules)
        return cls(product, data)

    def __init__(self, product, data: Optional[geopandas.GeoDataFrame] = None):
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
                "Combining two Index objects requires them to refer to the"
                " same product."
            )
        product = self.product
        if self.data is None:
            data = other.data
        else:
            columns = [
                "start_time",
                "end_time",
                "filename",
                "primary_index_name",
                "primary_index_start",
                "primary_index_end",
                "secondary_index_name",
                "secondary_index_start",
                "secondary_index_end",
            ]
            data = pd.concat(
                [self.data, other.data],
            ).drop_duplicates(subset=columns, ignore_index=True)
        return Index(product, data)

    @property
    def time_range(self):
        """
        The time range coverged by the granules in the index.
        """
        return TimeRange(
            self.data.start_time.min(),
            self.data.end_time.max()
        )

    @property
    def granules(self):
        """
        Iterator over all granules in index.
        """
        if self.data is None:
            return StopIteration
        granules = _dataframe_to_granules(self.product, self.data)
        for granule in granules:
            yield granule

    def __len__(self):
        """The number of granules in the index."""
        if self.data is None:
            return 0
        return self.data.shape[0]

    def insert(self, granules: Union[List[Granule], FileRecord]) -> None:
        """
        Insert a list of granules or granules from a file into index.

        Args:
            granules: A list of granules or optionally a FileRecord pointing
                to a file to insert into the index.
        """
        if isinstance(granules, Granule):
            granules = [granules]
        elif isinstance(granules, FileRecord):
            granules = Granule.from_file_record(granules)

        new_data = _granules_to_dataframe(granules)
        if self.data is None:
            self.data = new_data
        else:
            columns = [
                "start_time",
                "end_time",
                "filename",
                "primary_index_name",
                "primary_index_start",
                "primary_index_end",
                "secondary_index_name",
                "secondary_index_start",
                "secondary_index_end",
            ]
            self.data = pd.concat(
                [self.data, new_data],
            ).drop_duplicates(subset=columns, ignore_index=True)

    def __repr__(self):
        if self.data is None:
            return f"<Index of '{self.product.name}' containing " f"no data entries>"
        return (
            f"<Index of '{self.product.name}' containing "
            f"{self.data.start_time.size} entries>"
        )

    def find_local_path(self, file_record: FileRecord) -> Optional[None]:
        """
        Find the local path corresponding to a given file record.

        Args:
            file_record: A FileRecord pointing to a given data file.

        Return:
            A pathlib.Path object pointing to the file or 'None' if the
            file is not available from the index.
        """
        if self.data is None:
            return None

        inds = self.data.filename == file_record.filename
        if np.any(inds):
            path = Path(self.data.loc[inds].iloc[0].local_path)
            if path == "":
                return None
            return path
        return None

    def find(
        self, time_range: Optional[TimeRange] = None, roi: Optional[Geometry] = None
    ):
        """
        Find entries in Index within given time range and location.
        """
        if self.data is None:
            return []

        if time_range is None and roi is None:
            return _dataframe_to_granules(self.product, self.data)

        if time_range is None:
            selected = self.data
        else:
            if not isinstance(time_range, TimeRange):
                time_range = TimeRange(time_range, time_range)
            selected = self.data.loc[
                ~(
                    (self.data.start_time > time_range.end)
                    | (self.data.end_time < time_range.start)
                )
            ]

        if roi is None:
            return _dataframe_to_granules(self.product, selected)

        roi = roi.to_shapely()
        indices = selected.intersects(roi)

        return _dataframe_to_granules(self.product, selected.loc[indices])

    def save(self, db_path: Path, append: bool = False):
        """
        Save an index.

        Args:
            db_path: Path to a database to which to save the index.
            append: Whether or not to append index data in the case that
                the database already contains an index for this
                index's product.
        """
        if self.data is None:
            raise ValueError("Cannot save an empty index.")
        db_path = Path(db_path)
        database.save_index_data(self.product, self.data, db_path, append=append)


    def subset(
        self, time_range: Optional[TimeRange] = None, roi: Optional[Geometry] = None
    ) -> "Index":
        """
        Subset index to a given time range or region of interest.

        Args:
            time_range: A TimeRange object defining the time range.
            roi: A Geometry object defining the ROI.

        Return:
            A index object containing only entries withing the given time
            range or ROI.
        """
        if time_range is None and roi is None:
            return self

        if time_range is None:
            selected = self.data
        else:
            if not isinstance(time_range, TimeRange):
                time_range = TimeRange(time_range, time_range)
            selected = self.data.loc[
                ~(
                    (self.data.start_time > time_range.end)
                    | (self.data.end_time < time_range.start)
                )
            ]

        if roi is None:
            return Index(self.product, selected)
        roi = roi.to_shapely()
        indices = selected.intersects(roi)

        return Index(self.product, self.data.loc[indices])

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
        matches_r_1 = match_1[1]
        matches_r_2 = match_2[1]

        adj_1 = []
        adj_2 = []
        merged_r = []

        for g_1 in matches_r_1:
            for g_2 in matches_r_2:
                if g_1.is_adjacent(g_2):
                    adj_1.append(g_1)
                    adj_2.append(g_2)
                    merged_r.append(g_1.merge(g_2))

        merged_r = set(merged_r)
        [matches_r_1.remove(granule) for granule in adj_1]
        [matches_r_2.remove(granule) for granule in adj_2]

        results = []
        if len(matches_r_1) > 0:
            results.append((match_1[0], matches_r_1))
        if len(merged_r) > 0:
            results.append((match_1[0].merge(match_2[0]), merged_r))
        if len(matches_r_2) > 0:
            results.append((match_2[0], matches_r_2))
        return results

    return [match_1, match_2]


def _find_matches_rec(
    prod_l, index_data_l, prod_r, index_data_r, time_diff, merge=True, done_queue=None
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

        split_1_l = index_data_l.iloc[: n_l // 2]
        split_2_l = index_data_l.iloc[n_l // 2 :]

        split_1_start = split_1_l.start_time.iloc[0] - time_diff
        split_1_end = split_1_l.end_time.iloc[-1] + time_diff
        inds_1_r = (index_data_r.end_time > split_1_start) * (
            index_data_r.start_time < split_1_end
        )
        split_1_r = index_data_r.loc[inds_1_r]

        split_2_start = split_2_l.start_time.iloc[0] - time_diff
        split_2_end = split_2_l.end_time.iloc[-1] + time_diff
        inds_2_r = (index_data_r.end_time > split_2_start) * (
            index_data_r.start_time < split_2_end
        )
        split_2_r = index_data_r.loc[inds_2_r]

        matches_1 = _find_matches_rec(
            prod_l,
            split_1_l,
            prod_r,
            split_1_r,
            time_diff,
            merge=merge,
            done_queue=done_queue,
        )
        matches_2 = _find_matches_rec(
            prod_l,
            split_2_l,
            prod_r,
            split_2_r,
            time_diff,
            merge=merge,
            done_queue=done_queue,
        )

        if len(matches_1) == 0:
            return matches_2

        if len(matches_2) == 0:
            return matches_1

        if merge:
            return (
                matches_1[:-1]
                + merge_matches(matches_1[-1], matches_2[0])
                + matches_2[1:]
            )
        return matches_1 + matches_2

    start_time = index_data_l.start_time.iloc[0] - time_diff
    end_time = index_data_l.end_time.iloc[0] + time_diff

    matches = (index_data_r.end_time > start_time) * (
        index_data_r.start_time < end_time
    )
    selected = index_data_r.loc[matches]
    matches = selected.intersects(index_data_l.geometry.iloc[0])
    granules_r = _dataframe_to_granules(prod_r, selected.loc[matches])

    if len(granules_r) == 0:
        if done_queue is not None:
            done_queue.put(1)
        return []

    if merge:
        granules_r = merge_granules(granules_r)

    granule_l = _dataframe_to_granules(prod_l, index_data_l)[0]

    if done_queue is not None:
        done_queue.put(1)
    return [(granule_l, set(granules_r))]


def find_matches(
    index_l: Index,
    index_r: Index,
    time_diff: Optional[np.timedelta64] = None,
    n_processes: Optional[int] = None,
    merge: bool = True,
) -> List[Tuple[Granule, Set[Granule]]]:
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
            merge=merge,
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
        inds_r = (index_r.data.end_time > start_time) * (
            index_r.data.start_time < end_time
        )
        index_data_r = index_r.data.loc[inds_r]

        tasks.append(
            pool.submit(
                _find_matches_rec,
                index_l.product,
                index_data_l,
                index_r.product,
                index_data_r,
                time_diff=time_diff,
                merge=merge,
                done_queue=done_queue,
            )
        )

    assert ind_end == n_granules_l

    matches = []
    with rich.progress.Progress() as prog:
        prog_task = prog.add_task(
            description="Matching indices: ", start=True, total=n_granules_l
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
                    matches[:-1]
                    + merge_matches(matches[-1], matches_t[0])
                    + matches_t[1:]
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
            granules = match[ind]
            if isinstance(granules, Granule):
                granules = [granules]

            for granule in granules:
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
                index=match_indices,
            )
        )
    return tuple(dframes)
