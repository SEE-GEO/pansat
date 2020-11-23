"""
pansat.download.commandline
===============================
The ``commandline´´ submodule allows to download data products from commandline using the argparse module.

The following flags can be used:
    --type
    --pm
    --product/-prod
    --starttime/-t0
    --endtime/-t1
    --variable/-var
    --domain/-d


"""
import argparse
import datetime
import pkgutil
import importlib
import sys
import pansat.products


def download():

    ################################################################
    # define helpstrings, i.e. what is shown for the --help/-h flag
    ################################################################

    helpstring_t0 = "start of time interval in ISO 8601 format (YY-MM-DDThh:mm:ss)"
    helpstring_t1 = "end of time interval in ISO 8601 format (YY-MM-DDThh:mm:ss)"
    helpstring_type = "data type (satellite/reanalysis)"
    helpstring_pm = "satellite (e.g. Cloudsat)/ model (e.g ERA5)"
    helpstring_product = "product to download"
    helpstring_variable = "variable(s) for reanalysis"
    helpstring_domain = (
        "data domain of model data in latitude and longitude, if not specified"
        + " a global grid is used, if specified four arguments (lat1, lat2, lon1, lon2)"
        + " are required, with lat1<lat2, lon1<lon2"
    )

    parser = argparse.ArgumentParser()

    ##############################################################################################
    # definition of flags
    ##############################################################################################
    parser.add_argument('--list', action="store_true", help = "list available providers/products")


    if sys.version_info >= (3, 7):
        parser.add_argument(
            "-t0",
            "--starttime",
            type=datetime.datetime.fromisoformat,
            help=helpstring_t0,
        )
        parser.add_argument(
            "-t1", "--endtime", type=datetime.datetime.fromisoformat, help=helpstring_t1
        )
    else:  # i.e. for Python 3.6
        parser.add_argument("-t0", "--starttime", help=helpstring_t0)
        parser.add_argument("-t1", "--endtime", help=helpstring_t1)

    parser.add_argument(
        "--type", choices=["satellite", "reanalysis"], help=helpstring_type
    )
    parser.add_argument("--pm", help=helpstring_pm)
    parser.add_argument("-prod", "--product", nargs="+", help=helpstring_product)
    parser.add_argument("-var", "--variable", nargs="+", help=helpstring_variable)
    parser.add_argument("-d", "--domain", nargs=4, type=float, help=helpstring_domain)
    args = parser.parse_args()
    
    if args.list:
        modnames = []
        package = pansat.download
        modlen=len("pansat.download.providers.")
        for importer, modname, ispkg in pkgutil.walk_packages(
           path=package.__path__, prefix=package.__name__ + ".", onerror=lambda x: None
           ):
            if "providers." in modname and "_provider" not in modname:
                modnames.append(modname)
                print('Provider: '+str(modname[modlen:]))
                module = importlib.import_module(modname)
                products=getattr(module, str(modname[modlen:]).upper()+"_PRODUCTS")

                print('available products:')
                if isinstance(products,dict):
                    for k, v in products.items():
                        print(k, v)
                else:    
                    for i in range(0,len(products)):
                        print(products[i])
        return
    
    
    #################################################################################
    # consistency checks and conversion of times into datetime-format
    #################################################################################

    if (args.starttime and not args.endtime) or (args.endtime and not args.starttime):
        parser.error("both -t0 and -t1 need to be specified at the same time")

    if not args.pm:
        parser.error("no platform/model chosen")

    if not args.product:
        parser.error("no data product chosen")

    if sys.version_info < (3, 7):
        args.starttime = datetime.datetime.strptime(args.starttime, "%Y-%m-%dT%H:%M:%S")
        args.endtime = datetime.datetime.strptime(args.endtime, "%Y-%m-%dT%H:%M:%S")

    if str(args.type) == "reanalysis" and not args.variable:
        parser.error("no reanalysis variable chosen")

    if str(args.type) == "satellite" and len(args.product) > 1:
        parser.error("satellite products expect only one argument")

    if str(args.type) == "reanalysis" and len(args.product) != 2:
        parser.error("reanalysis products expect two arguments")

    ##############################################################################################
    # loading download functions and starting download
    ##############################################################################################
    modnames = []
    package = pansat.products
    for importer, modname, ispkg in pkgutil.walk_packages(
        path=package.__path__, prefix=package.__name__ + ".", onerror=lambda x: None
    ):
        modnames.append(modname)

    if "pansat.products." + str(args.type) + "." + str(args.pm) in modnames:
        module = importlib.import_module(
            "pansat.products." + str(args.type) + "." + str(args.pm)
        )
        if "l" + str(args.product[0]) in dir(module):
            productfunc = getattr(module, "l" + str(args.product[0]))
        elif "ERA5Product" in dir(module):
            era_product = getattr(module, "ERA5Product")
            if not args.domain:#without given spatial domain (global)
                productfunc = era_product(
                    str(args.product[0]), str(args.product[1]), args.variable
                )
            else:
                productfunc = era_product(
                    str(args.product[0]), str(args.product[1]), args.variable, args.domain
                )
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

    files = productfunc.download(args.starttime, args.endtime)
