from .network import createNetworks, saveNetworkEdgesCSV,k_DefaultKeptItems
from .utilities import saveJSONLines, entitiesFromJSONLines, aggregateEntities, filterDuplicates
from .api import OpenAlexAPI
from pathlib import Path
import pandas as pd
import json
import xnetwork as xn
from tqdm.auto import tqdm


k_EntityTypes = ["works", "institutions", "authors", "concepts", "venues"]

allowedOutputNetworkFormats = {
    ".edgelist",
    ".gml",
    ".xnet",
}

def saveNetwork(network,filename):
    """Saves a network to a file.
    
    Parameters
    ----------
    network : networkx.Graph
        Network to be saved.
    filename : str or pathlib.Path
        Path to the file where the network will be saved.
    **kwargs
        Additional arguments to be passed to the saveNetworkEdgesCSV.
    """
    filename = Path(filename)
    if filename.suffix.lower() == ".edgelist":
        saveNetworkEdgesCSV(network,filename)
    elif filename.suffix.lower() == ".gml":
        network.write_gml(str(filename.resolve()))
    elif filename.suffix.lower() == ".xnet":
        xn.igraph2xnet(network,filename)
    else:
        raise ValueError(f"Invalid network output format {filename.suffix}. Allowed formats are {allowedOutputNetworkFormats}")

def standaloneApp(
        entityType,
        email = "",
        filterQuery={},
        searchQuery="",
        sortQuery=[],
        inputQueryFile=None,
        outputJSONLFile=None,
        inputJSONLFile=None,
        citationNetworkOutputPath=None,
        coautorshipNetworkOutputPath=None,
        simplifyNetworks = True,
        coauthorshipNormalizedWeights = True,
        maxEntities = 10000,
        keptAttributes = k_DefaultKeptItems,
        ignoreAttributes = None,
        verbose = True,
        ignoreEntitiesLimitWarning=False,
        rateInterval=0.0,
):
    """Retrieves entities from the OpenAlex API and saves them to a JSON Lines file or networks.
    
    Parameters
    ----------
    entityType : str
        Type of entity to be retrieved from the OpenAlex API. Can be one of the following: "works", "institutions", "authors", "concepts", "venues".
    email : str, optional
        Email to be used in the OpenAlex API call for polite calls. The default is "".
    filterQuery : dict, list, or str, optional
        Dictionary of filters to be used in the OpenAlex API call. The keys are the names of the filters and the values are the values of the filters.
        alternatively. A list of strings parameters formatted as openalex API or a string can be used instead. The default is {}.
    searchQuery : str, optional
        Search term to be used in the OpenAlex API call. The default is "".
    sortQuery : list or str, optional
        List of sort terms to be used in the OpenAlex API call. Include ":desc" to the name to sort in descending order. Alternatively, a string with the OpenAlex API query can be provided instead. The default is [].
    inputQueryFile : str or pathlib.Path, optional
        Alternatively, a csv or tsv (tab-delimited) with headers can be provided with each line containing a query. Allowed columns are:
        "filter", "search", "sort" and "maxentities". Filter and sort entries are formated respectivelly as json dictionary and list or as OpenAlex API query strings, search are plain strings.
        Missing columns will assume default values. The default is None.
    outputJSONLFile : str or pathlib.Path, optional
        Path to the JSON Lines file where the entities will be saved. If None, the entities will not be saved to a JSON Lines file. The default is None.
    inputJSONLFile : str or pathlib.Path, optional
        Path to the saved JSON Lines files to be used instead of querying from the OpenAlex API. If None, the entities will be retrieved from the OpenAlex API. The default is None.
        Only supported for works entities.
    citationNetworkOutputPath : str or pathlib.Path, optional
        Path to a output file with extensions ".edgelist", ".gml", or ".xnet" where the citation network will be saved. If None, the citation network will not be saved. The default is None.
    coautorshipNetworkOutputPath : str or pathlib.Path, optional
        Path to a output file with extensions ".edgelist", ".gml", or ".xnet" where the coautorship network will be saved. If None, the coautorship network will not be saved. The default is None.
    simplifyNetworks : bool, optional
        If True, the coauthorship network edges will be aggregated, resulting in no multiple edges. The default is True.
    coauthorshipNormalizedWeights : bool, optional
        If True, the coauthorship network will have normalized weights, i.e., the contribution of a paper to a connection weight is the inverse of the number of authors in the paper. The default is True.
    maxEntities : int, optional
        Maximum number of entities to be retrieved from the OpenAlex API. Use -1 to retrieve all entities. The default is 10000.
    keptAttributes : list, optional
        List of attributes to be kept in the entities. The default is openalexnet.network.k_DefaultKeptItems.
    ignoreAttributes : list, optional
        List of attributes to be ignored in the entities. The default is None.
    verbose : bool, optional
        If True, the progress of the retrieval will be printed. The default is True.
    ignoreEntitiesLimitWarning : bool, optional
        If True, the warning about the maximum number of entities will be ignored. The default is False.
    rateInterval : float, optional
        Interval in seconds between API calls. The default is 0.0.
    """

    if entityType not in k_EntityTypes:
        raise ValueError(f"entityType must be one of the following: {', '.join(k_EntityTypes)}")
    
    openalex = OpenAlexAPI(email=email)

    if inputQueryFile is not None:
        inputQueryFile = Path(inputQueryFile)
        if not inputQueryFile.exists():
            raise FileNotFoundError(f"inputQueryFile {inputQueryFile} does not exist")
        if inputQueryFile.suffix not in {".csv", ".tsv"}:
            raise ValueError(f"inputQueryFile {inputQueryFile} must have a .csv or .tsv extension")
        if inputQueryFile.suffix == ".tsv":
            delimiter = "\t"
        else:
            delimiter = ","
        queries = pd.read_csv(inputQueryFile, delimiter=delimiter,na_values=None,keep_default_na=False)

        if "filter" not in queries.columns:
            queries["filter"] = ""
        if "search" not in queries.columns:
            queries["search"] = ""
        if "sort" not in queries.columns:
            queries["sort"] = ""
        if "maxentities" not in queries.columns:
            queries["maxentities"] = maxEntities
        
        if(verbose):
            print(f"Reading queries from {inputQueryFile}")


        queriesList = []
        for index, row in queries.iterrows():
            queryParameters = {}
            queryParameters["maxEntities"] = maxEntities
            if row["maxentities"] is not None:
                queryParameters["maxEntities"] = int(row["maxentities"])
            else:
                queryParameters["maxEntities"] = maxEntities

            if (row["filter"]):
                queryParameters["filter"] = row["filter"]

            if (row["search"]):
                queryParameters["search"] = row["search"]
            
            if (row["sort"]):
                queryParameters["sort"] = row["sort"]
            
            queryParameters["ignoreEntitiesLimitWarning"] = ignoreEntitiesLimitWarning
            queryParameters["rateInterval"] = rateInterval
            
            entities = openalex.getEntities(entityType,**queryParameters)
            
            if(verbose):
                entities = tqdm(entities, desc=f"Retrieving query {index+1}/{len(queries)}",leave=False)
            queriesList.append(entities)
        if(verbose):
            queriesList = tqdm(queriesList, desc=f"Retrieving queries")
        
        allEntities = aggregateEntities(queriesList)
    elif inputJSONLFile:
        if entityType != "works":
            raise ValueError(f"inputJSONLFile is only supported for works entities")
        allEntities = entitiesFromJSONLines(inputJSONLFile)
    else:
        queryParameters = {}
        queryParameters["maxEntities"] = maxEntities
        if (filterQuery):
            queryParameters["filter"] = filterQuery

        if (searchQuery):
            queryParameters["search"] = searchQuery
        
        if (sortQuery):
            queryParameters["sort"] = sortQuery
        
        queryParameters["ignoreEntitiesLimitWarning"] = ignoreEntitiesLimitWarning
        queryParameters["rateInterval"] = rateInterval

        if(verbose):
            print(f"Retrieving entities of type {entityType}")
        allEntities = openalex.getEntities(entityType,**queryParameters)
        if(verbose):
            allEntities = tqdm(allEntities, desc=f"Retrieving entities")
    

    if(outputJSONLFile and (citationNetworkOutputPath or coautorshipNetworkOutputPath)):
        allEntities = list(allEntities) # Convert to list to reuse

    if outputJSONLFile:
        saveJSONLines(allEntities, outputJSONLFile)

    
    
    if(citationNetworkOutputPath or coautorshipNetworkOutputPath):
        networkTypes = []
        
        if(citationNetworkOutputPath):
            networkTypes.append("citation")
        if(coautorshipNetworkOutputPath):
            networkTypes.append("coauthorship")

        networks = createNetworks(allEntities,
                        networkTypes=networkTypes,
                        simplifyNetworks=simplifyNetworks,
                        keptAttributes=keptAttributes,
                        ignoreAttributes=ignoreAttributes,
                        showProgress=verbose)
        if citationNetworkOutputPath:
            saveNetwork(networks["citation"], citationNetworkOutputPath)

        if coautorshipNetworkOutputPath:
            if(coauthorshipNormalizedWeights):
                networks["coauthorship"].es["weight"] = networks["coauthorship"].es["normalized_weight"]
            else:
                networks["coauthorship"].es["weight"] = networks["coauthorship"].es["count"]
            saveNetwork(networks["coauthorship"], coautorshipNetworkOutputPath)






def processCMDParameters():
    import sys
    import argparse
    import pathlib
    from argparse import RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        prog="openalexnet",
        description='Retrieves entities from the OpenAlex API and saves them to a JSON Lines file or networks.',
        formatter_class=RawDescriptionHelpFormatter,
        epilog=\
            """
Examples:
---------
Retrieve works as journal-articles with the term "complex networks" and save them to a JSON Lines, citation network and coauthorship network:
>openalexnet -t works -f \"type:journal-article\" -s \"complex networks\" -r \"cited_by_count:desc\" -o works.jsonl -c citation_network.gml -a coauthorship_network.gml

Reuse previusly saved works and save them to a citation network and coauthorship network in edgelist format:
>openalexnet -t works -i works.jsonl -c citation_network.edgelist -a coauthorship_network.edgelist

Use a query file to retrieve works and save them to a JSON Lines file:
>openalexnet -t works -q query.csv -o works.jsonl

query.csv:
filter,search,sort,maxentities
"type:journal-article",\"""complex networks\""","cited_by_count:desc",10000
"type:journal-article",\"""network science\""","cited_by_count:desc",10000

API Reference
------
See https://docs.openalex.org for supported filters, sort terms and attributes.
"""
        )
    parser.add_argument(
        "-t",
        "--entitytype",
        type=str,
        help='Type of entity to be retrieved from the OpenAlex API. Can be one of the following: "works", "institutions", "authors", "concepts", "venues".'
    )


    parser.add_argument(
        "-e",
        "--email",
        type=str,
        help='Email to be used in the OpenAlex API call for polite calls. Default is "".'
    )


    parser.add_argument(
        "-f",
        "--filter",
        type=str,
        help='Comma separated filter entries formatted as key:value to be used in the OpenAlex API call. \
        The keys are the names of the filters and the values are the values of the filters. Example: -f \"type:journal-article,author.id:A2420755856\". The default is "".'
    )

    parser.add_argument(
        "-s",
        "--search",
        type=str,
        help='Search term to be used in the OpenAlex API call. Example: -s \"complex systems\". The default is "".'
    )

    parser.add_argument(
        "-r",
        "--sort",
        type=str,
        help='Comma separated sort entries formatted as key[:desc] to be used in the OpenAlex API call. \
        Include ":desc" to the name to sort in descending order. Example: -s \"cited_by_count:desc\" The default is "".'
    )

    parser.add_argument(
        "-q",
        "--queryfile",
        type=str,
        help='Alternatively, a csv or tsv (tab-delimited) with headers can be provided with each line containing a query. \
        Allowed columns are: "filter", "search", "sort" and "maxentities". Filter and sort entries are formated respectivelly as json dictionary and list or as OpenAlex API query strings, search are plain strings. \
        Missing columns will assume default values. The default is None.'
    )

    parser.add_argument(
        "-o",
        "--outputfile",
        type=pathlib.Path,
        help='Path to the JSON Lines file where the entities will be saved. If None, the entities will not be saved to a JSON Lines file. The default is None.'
    )

    parser.add_argument(
        "-i",
        "--inputfile",
        type=pathlib.Path,
        help='Path to the saved JSON Lines files to be used instead of querying from the OpenAlex API. If None, the entities will be retrieved from the OpenAlex API. The default is None. \
        Only supported for works entities.'
    )

    parser.add_argument(
        "-c",
        "--citationfile",
        type=pathlib.Path,
        help='Path to a output file with extensions ".edgelist", ".gml", or ".xnet" where the citation network will be saved. If None, the citation network will not be saved. The default is None.'
    )

    parser.add_argument(
        "-a",
        "--coauthorfile",
        type=pathlib.Path,
        help='Path to a output file with extensions ".edgelist", ".gml", or ".xnet" where the coautorship network will be saved. If None, the coautorship network will not be saved. The default is None.'
    )

    parser.add_argument(
        "-n",
        "--no_simplenetworks",
        action='store_true',
        help='If enabled the coauthorship network edges will not be aggregated, resulting in multiple edges. The default is disabled.'
    )

    parser.add_argument(
        "-w",
        "--countweights",
        action='store_true',
        help='If enabled the coauthorship network will have non-normalized weights, i.e., the contribution of a paper to a connection weight is 1.0, otherwise the contribution is the inverse of the number of authors in the paper. The default is disabled.'
    )

    parser.add_argument(
        "-m",
        "--maxentities",
        type=int,
        help='Maximum number of entities to be retrieved from the OpenAlex API. Use -1 to retrieve all entities. The default is 10000.'
    )

    parser.add_argument(
        "-k",
        "--keptattributes",
        type=str,
        help='Comma separated list of attributes to be kept in the entities. The default is {k_DefaultKeptItems}.'
    )

    parser.add_argument(
        "-g",
        "--ignoreattributes",
        type=str,
        help='Comma separated list of attributes to be ignored in the entities. The default is None.'
    )

    parser.add_argument(
        "-Q",
        "--quiet",
        action='store_true',
        help='If enabled the progress of the retrieval will not be printed. The default is disabled.'
    )

    parser.add_argument(
        "-l",
        "--ignorelimitwarning",
        action='store_true',
        help='If enabled the warning about the maximum number of entities will be ignored. The default is disabled.'
    )

    parser.add_argument(
        "-d",
        "--rateinterval",
        type=float,
        help='Interval in seconds between API calls. The default is 0.0.'
    )
    

    args = parser.parse_args()

    #if no arguments are provided, print help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    parameters = {}
    #validate parameters
    if args.entitytype is None:
        raise ValueError("Entity type must be provided.")
    
    if args.entitytype not in k_EntityTypes:
        raise ValueError("Entity type must be one of the following: " + str(k_EntityTypes))
    
    if args.filter or args.search or args.sort:
        if args.queryfile:
            raise ValueError("Filter, search and sort parameters cannot be used with a query file.")
    
    if args.keptattributes is None:
        parameters["keptAttributes"] = k_DefaultKeptItems
    else:
        parameters["keptAttributes"] = args.keptattributes.split(',')

    if args.ignoreattributes:
        parameters["ignoreAttributes"] = args.ignoreattributes.split(',')
    else:
        parameters["ignoreAttributes"] = []

    if args.maxentities is None:
        parameters["maxEntities"] = 10000
    else:
        parameters["maxEntities"] = args.maxentities

    if args.rateinterval is None:
        parameters["rateInterval"] = 0.0
    else:
        parameters["rateInterval"] = args.rateinterval
    
    parameters["verbose"] = not args.quiet
    parameters["ignoreEntitiesLimitWarning"] = args.ignorelimitwarning
    parameters["simplifyNetworks"] = not args.no_simplenetworks
    parameters["coauthorshipNormalizedWeights"] = not args.countweights

    if args.queryfile:
        parameters["inputQueryFile"] = args.queryfile
    
    if args.inputfile:
        parameters["inputJSONLFile"] = args.inputfile

    if args.outputfile:
        parameters["outputJSONLFile"] = args.outputfile

    if args.citationfile:
        if (args.citationfile.suffix.lower() in allowedOutputNetworkFormats):
            parameters["citationNetworkOutputPath"] = args.citationfile
        else:
            raise ValueError("Citation network output file must have one of the following extensions: " + str(allowedOutputNetworkFormats))

    if args.coauthorfile:
        if (args.coauthorfile.suffix.lower() in allowedOutputNetworkFormats):
            parameters["coautorshipNetworkOutputPath"] = args.coauthorfile
        else:
            raise ValueError("Coauthorship network output file must have one of the following extensions: " + str(allowedOutputNetworkFormats))

    if args.filter:
        parameters["filterQuery"] = args.filter

    if args.search:
        parameters["searchQuery"] = args.search

    if args.sort:
        parameters["sortQuery"] = args.sort

    parameters["entityType"] = args.entitytype

    return parameters



def main():
    parameters = processCMDParameters()
    standaloneApp(**parameters)
    
