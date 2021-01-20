"""
pansat.download.commandline
===============================
The ``commandline`` submodule allows to download data products from commandline using the argparse module.

The following flags can be used:
   Download flags

   .. code-block::

      --type
      --pm
      --product/-prod
      --starttime/-t0
      --endtime/-t1
      --variable/-var
      --domain/-d
      --grid
   
   Other flags

   .. code-block::

      --list
      --add
      --listIDs


"""
import argparse
import datetime
import pkgutil
import importlib
import sys
import pansat.products


def download():
    """
    The function download() passes the commandline input to the respective download functions
    of the corresponding pansat.download submodules.
    """
    ################################################################
    # define helpstrings, i.e. what is shown for the --help/-h flag
    ################################################################

    helpstring_t0 = "start of time interval in ISO 8601 format (YYYY-MM-DDThh:mm:ss)"
    helpstring_t1 = "end of time interval in ISO 8601 format (YYYY-MM-DDThh:mm:ss)"
    helpstring_type = "data type (satellite/reanalysis)"
    helpstring_pm = (
        "satellite (e.g. Cloudsat)/ model (e.g ERA5)/ ground_based/ stations"
    )
    helpstring_product = "product to download"
    helpstring_variable = "variable(s) for reanalysis/station data"
    helpstring_domain = (
        "data domain of model data in latitude and longitude, if not specified"
        + " a global grid is used, if specified four arguments (lat1, lat2, lon1, lon2)"
        + " are required, with lat1<lat2, lon1<lon2"
    )
    helpstring_grid = "specifying spatial/temporal gridding of reanalysis data"
    helpstring_loc = "location of station in [lat,lon]"
    helpstring_name = "name of station"
    helpstring_add = "add account, requires arguments ``provider´´ and ``user_name´´"

    parser = argparse.ArgumentParser()

    ##############################################################################################
    # definition of flags
    ##############################################################################################
    parser.add_argument(
        "--list", action="store_true", help="list available providers/products"
    )
    parser.add_argument("--listIDs", action="store_true", help="list stored identities")
    parser.add_argument("--add", nargs=2, help=helpstring_add)

    parser.add_argument(
        "-t0",
        "--starttime",
        type=datetime.datetime.fromisoformat,
        help=helpstring_t0,
    )
    parser.add_argument(
        "-t1", "--endtime", type=datetime.datetime.fromisoformat, help=helpstring_t1
    )

    parser.add_argument(
        "--type",
        choices=["satellite", "reanalysis", "ground_based", "stations"],
        help=helpstring_type,
    )
    parser.add_argument("--pm", help=helpstring_pm)
    parser.add_argument("-prod", "--product", help=helpstring_product)
    parser.add_argument("-var", "--variable", nargs="+", help=helpstring_variable)
    parser.add_argument("--grid", nargs="+", help=helpstring_grid)
    parser.add_argument("-d", "--domain", nargs=4, type=float, help=helpstring_domain)
    parser.add_argument("-loc", "--location", nargs=2, type=float, help=helpstring_loc)
    parser.add_argument("--name", help=helpstring_name)
    parser.add_argument(
        "--recent", action="store_true", help="only download last month of IGRA data"
    )
    args = parser.parse_args()

    if args.list:
        modnames = []
        package = pansat.download
        modlen = len("pansat.download.providers.")
        for importer, modname, ispkg in pkgutil.walk_packages(
            path=package.__path__, prefix=package.__name__ + ".", onerror=lambda x: None
        ):
            if "providers." in modname and "_provider" not in modname:
                modnames.append(modname)
                print("Provider: " + str(modname[modlen:]))
                module = importlib.import_module(modname)
                products = getattr(module, str(modname[modlen:]).upper() + "_PRODUCTS")

                print("available products:")
                if isinstance(products, dict):
                    for k, v in products.items():
                        print(k, v)
                else:
                    for i in range(0, len(products)):
                        print(products[i])
        return

    if args.add:
        from pansat.download import accounts

        accounts.add_identity(args.add[0], args.add[1])
        return

    if args.listIDs:
        from pansat.download import accounts

        identities = accounts.get_identities()
        print("Identities are available for the following providers")
        for key in identities.keys():
            if key != "pansat":
                print(key)
        return

    #################################################################################
    # consistency checks to begin downloads
    #################################################################################

    if (args.starttime and not args.endtime) or (args.endtime and not args.starttime):
        parser.error("both -t0 and -t1 need to be specified at the same time")

    if not args.pm:
        parser.error("no platform/model chosen")

    if str(args.type) == "satellite" and not args.product:
        parser.error("no satellite data product chosen")

    if str(args.type) == "reanalysis" and not args.variable:
        parser.error("no reanalysis variable chosen")

    if str(args.type) == "reanalysis" and not args.grid:
        parser.error("no grid variable for reanalysis data chosen")

    if str(args.pm) == "ncep" and len(args.grid) != 1:
        parser.error("NCEP data requires exactly one argument for gridding information")

    if str(args.pm) == "ERA5" and len(args.grid) != 2:
        parser.error(
            "ERA5 data requires exactly two arguments for gridding information"
        )

    ##############################################################################################
    # loading download functions and starting download
    ##############################################################################################
    modnames = []
    package = pansat.products
    for importer, modname, ispkg in pkgutil.walk_packages(
        path=package.__path__, prefix=package.__name__ + ".", onerror=lambda x: None
    ):
        modnames.append(modname)

    modnames.append("pansat.products.ground_based.opera")
    modnames.append("pansat.products.stations.igra")

    if "pansat.products." + str(args.type) + "." + str(args.pm) in modnames:
        module = importlib.import_module(
            "pansat.products." + str(args.type) + "." + str(args.pm)
        )
        if "l" + str(args.product) in dir(module):
            productfunc = getattr(module, "l" + str(args.product))
        elif str(args.product) in dir(module):
            productfunc = getattr(module, str(args.product))
        elif "ERA5Product" in dir(module):
            era_product = getattr(module, "ERA5Product")
            if not args.domain:  # without given spatial domain (global)
                productfunc = era_product(
                    str(args.grid[0]), str(args.grid[1]), args.variable
                )
            else:
                productfunc = era_product(
                    str(args.grid[0]),
                    str(args.grid[1]),
                    args.variable,
                    args.domain,
                )
        elif "NCEPReanalysis" in dir(module):
            ncep_product = getattr(module, "NCEPReanalysis")
            productfunc = ncep_product(str(args.variable[0]), str(args.grid[0]))
        elif "IGRASoundings" in dir(module):
            igra_product = getattr(module, "IGRASoundings")
            if args.location:
                productfunc = igra_product(args.location)
            elif args.name:
                productfunc = igra_product(args.name)
            elif args.variable:
                productfunc = igra_product(variable=str(args.variable[0]))
            else:
                parser.error("can't download IGRA data with given arguments")
        else:
            parser.error("product " + str(args.product) + " not implemented")

    else:
        parser.error(
            "submodule pansat.products."
            + str(args.type)
            + "."
            + str(args.pm)
            + " is not implemented"
        )

    if args.pm == "ncep":
        args.starttime = int(args.starttime.year)
        args.endtime = int(args.endtime.year)

    if args.type != "stations":
        files = productfunc.download(args.starttime, args.endtime)
    else:
        if not args.variable:
            files = productfunc.download()
        else:
            if not args.recent:
                files = productfunc.download()
            else:
                files = productfunc.download("recent")
