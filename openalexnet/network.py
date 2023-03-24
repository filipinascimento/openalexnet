

from tqdm.auto import tqdm
import igraph as ig
import json
from pathlib import Path
import pandas as pd

k_DefaultKeptItems={
    "id",
    "doi",
    "title",
    "display_name",
    "publication_year",
    "publication_date",
    "type",
    "authorships",
    "concepts",
    "host_venue",
}

def preprocessAttributes(attributes, keptAttributes={}, ignoreAttributes={}):
    """
    Preprocess attributes to be transfered to the network by converting non-numeric and non-strings to json.

    Parameters
    ----------
    attributes : dict
        Dictionary of attributes.
    keptAttributes : list (default to None)
        List of attributes to keep in the network. If None, all attributes are transfered to the network.
    ignoreAttributes : list (default: None)
        List of attributes to ignore in the network. This filter is applied after selecting the attributes to be kept.

    Returns
    -------
    attributes : dict
        Dictionary of attributes.
    """

    if keptAttributes is not None:
        attributes = {k:v for k,v in attributes.items() if k in keptAttributes}
    if ignoreAttributes is not None:
        attributes = {k:v for k,v in attributes.items() if k not in ignoreAttributes}
    for k,v in attributes.items():
        if not isinstance(v,(int,float,str)):
            attributes[k] = json.dumps(v)
    return attributes

def openAlexID2Int(openAlexID):
    """
    Convert OpenAlex ID to integer.

    Parameters
    ----------
    openAlexID : str
        OpenAlex ID.

    Returns
    -------
    int
        Integer corresponding to the OpenAlex ID.
    """
    return int(openAlexID.split("/")[-1][1:])

def int2OpenAlexID(intID,entityType="works"):
    """
    Convert integer to OpenAlex ID.

    Parameters
    ----------
    intID : int
        Integer represensting the OpenAlex ID.
    entityType : str
        Type of entity supported by the OpenAlex API.

    Returns
    -------
    str
        OpenAlex ID corresponding to the integer.
    """
    letter = entityType[0].upper()
    return f"https://openalex.org/{letter}{intID}"

def createNetworks(workEntities, networkTypes=["citation", "coauthorship"], simplifyNetworks=True, keptAttributes=k_DefaultKeptItems, ignoreAttributes=None, showProgress=True):
    """
    Create a igraph network from a list of work entities based on citations.

    Parameters
    ----------
    entities : iterator 
        Iterator of works entities.
    networkTypes : str or list
        Type of network to create. Can be "citation" or "coauthorship".
    simplifyNetworks : bool (default: True)
        Simplify the coauthorship networks by removing multiple edges.
    keptAttributes : list (default: k_DefaultKeptItems)
        List of attributes to keep in the network. If None, all attributes are transfered to the network.
    ignoreAttributes : list (default: None)
        List of attributes to ignore in the network. This filter is applied after selecting the attributes to be kept.
    showProgress : bool
        Show progress bar.
    Returns
    -------
    network : igraph.Graph
        The network.

    Notes
    -----
    Attributes are transfered to the network as vertex or edge attributes. Attributes that are not numeric or string are converted to json.

    k_DefaultKeptItems = [
        "id",
        "doi",
        "title",
        "display_name",
        "publication_year",
        "publication_date",
        "type",
        "authorships",
        "concepts",
        "host_venue",
    ]
    
    """

    keptAttributes = set() if keptAttributes is None else set(keptAttributes)
    ignoreAttributes = set() if ignoreAttributes is None else set(ignoreAttributes)

    createCitationNetwork = "citation" in networkTypes
    createCoauthorshipNetwork = "coauthorship" in networkTypes

    # add vertices and attributes
    if showProgress:
        entitiesTQDM = tqdm(workEntities,desc="Extracting edges and attributes",leave=False)
    else:
        entitiesTQDM = workEntities
    
    verticesAttributes = {} # {attribute:[list of values]}
    oaID2Index = {} # {vertex:ID}
    index2OaID = [] # {ID:vertex}

    if(createCitationNetwork):
        verticesReferences = [] # [list of references Ids]
    
    if(createCoauthorshipNetwork):
        verticesAuthorData = [] # [list of authors Ids]

    for entity in entitiesTQDM:
        oaID = openAlexID2Int(entity["id"])
        #ignore duplicates
        if(oaID in oaID2Index):
            continue
        oaID2Index[oaID] = len(index2OaID)
        index2OaID.append(oaID)
        attributes = preprocessAttributes(entity,keptAttributes,ignoreAttributes)
        if(createCitationNetwork):
            verticesReferences.append([openAlexID2Int(entry) for entry in entity["referenced_works"]])
        if(createCoauthorshipNetwork):
            verticesAuthorData.append({"authorships":entity["authorships"]})
            verticesAuthorData[-1]["work_id"] = oaID
            verticesAuthorData[-1]["work_year"] = entity["publication_year"]
            
        for k,v in attributes.items():
            if k not in verticesAttributes:
                verticesAttributes[k] = []
            verticesAttributes[k].append(v)
    
    results = {}
    if(createCitationNetwork):
        citationEdges = []
        for vertexIndex,references in enumerate(verticesReferences):
            for reference in references:
                if(reference in oaID2Index):
                    citationEdges.append((vertexIndex,oaID2Index[reference]))
        g = ig.Graph(n=len(index2OaID),edges=citationEdges,directed=True,vertex_attrs=verticesAttributes)
        results["citation"] = g
    
    if(createCoauthorshipNetwork):
        authorID2Index = {} # {authorID:vertex}
        index2AuthorID = [] # {vertex:authorID}

        authorAttributes = {
            "id": [],
            "display_name": [],
            "orcid": [],
        }

        coauthorshipEdgeAttributes = {
            "workID": [],
            "workYear": [],
            "normalized_weight": [],
            "count": [],
        }

        authorInstitutions = []
        authorInstitutionsIDs = []

        if(showProgress):
            authorsTQDM = tqdm(verticesAuthorData,desc="Generating coauthorship network",leave=False)
        else:
            authorsTQDM = verticesAuthorData
        
        coauthorshipEdges = []
        for vertexIndex,authorEntries in enumerate(authorsTQDM):
            # ignore works with a single author
            if(len(authorEntries)<=1):
                continue
            authorIndices = []
            for authorEntry in authorEntries["authorships"]:
                author = authorEntry["author"]
                if(not author):
                    continue
                if not author["id"]:
                    import warnings
                    warnings.warn(f"Author with data {author} has no ID. Ignoring.",stacklevel = 2)
                    continue
                authorID = openAlexID2Int(author["id"])
                if(authorID not in authorID2Index):
                    authorID2Index[authorID] = len(index2AuthorID)
                    index2AuthorID.append(authorID)
                    authorInstitutions.append(set())
                    authorInstitutionsIDs.append(set())
                    authorAttributes["id"].append(authorID)
                    authorDisplayName = author["display_name"]
                    if(not authorDisplayName):
                        authorDisplayName=""
                    authorAttributes["display_name"].append(authorDisplayName)
                    orcidData = author["orcid"]
                    if(not orcidData):
                        orcidData=""
                    authorAttributes["orcid"].append(orcidData)
                authorIndex = authorID2Index[authorID]
                if(authorEntry["institutions"]):
                    authorInstitutionsIDs[authorIndex].update({openAlexID2Int(entry["id"]) for entry in authorEntry["institutions"] if "id" in entry and entry["id"]})
                    authorInstitutions[authorIndex].update({entry["display_name"] for entry in authorEntry["institutions"] if "display_name"in entry and entry["display_name"]})
                authorIndices.append(authorIndex)
            workID = authorEntries["work_id"]
            weight = 1.0/len(authorEntries)

            for startIndex,fromAuthorIndex in enumerate(authorIndices):
                for toAuthorIndex in authorIndices[:startIndex]:
                    coauthorshipEdgeAttributes["workID"].append(workID)
                    coauthorshipEdgeAttributes["workYear"].append(authorEntries["work_year"])
                    coauthorshipEdgeAttributes["normalized_weight"].append(weight)
                    coauthorshipEdgeAttributes["count"].append(1)
                    edgeTuple = (fromAuthorIndex,toAuthorIndex)
                    coauthorshipEdges.append((min(edgeTuple),max(edgeTuple)))

        g = ig.Graph(n=len(index2AuthorID),edges=coauthorshipEdges,directed=False,vertex_attrs=authorAttributes,edge_attrs=coauthorshipEdgeAttributes)
        g.vs["Institutions"] = ["|".join(entries) for entries in authorInstitutions]
        g.vs["InstitutionsIDs"] = [",".join([str(entry) for entry in entries]) for entries in authorInstitutionsIDs]
        if(simplifyNetworks):
            g.es["firstYear"] = g.es["workYear"]
            g.es["lastYear"] = g.es["workYear"]
            g.es["weight"] = g.es["normalized_weight"]
            results["coauthorship"] = g.simplify(combine_edges={"firstYear":"min","lastYear":"max","normalized_weight":"sum","count":"sum","weight":"sum"})
        else:
            results["coauthorship"] = g
    return results





def saveNetworkEdgesCSV(g:ig.Graph, edgelistPath):
    """
    Save a network to an edgelist file and CSV for the attributes.

    Parameters
    ----------
    g : igraph.Graph
        The graph to save.
    destinationPath : str or pathlib.Path
        The path to save the files to.
    weight : str, optional
        The name of the edge attribute to use as the edge weight.
    
    Notes:
    ------
    The edgelist file will be saved to <edgelistPath> with the edge list.
    A CSV file will be saved to <edgelistPath w/o extension>_nodes.csv with the node attributes.
    A CSV file will be saved to <edgelistPath w/o extension>_edges.csv with the edge attributes.
    """
    edgelistPath = Path(edgelistPath)
    nodeCSVPath = edgelistPath.with_name(edgelistPath.stem+"_nodes.csv")
    edgeCSVPath = edgelistPath.with_name(edgelistPath.stem+"_edges.csv")
    edgeList = g.get_edgelist() # list of tuples

    if("weight" in g.es.attributes()):
        weights = g.es["weight"]
        edgeList = [(edge[0],edge[1],weights[edgeIndex]) for edgeIndex,edge in enumerate(edgeList)]
    
    with open(edgelistPath,"wt") as f:
        if("weight" in g.es.attributes()):
            for edge in edgeList:
                # print(edge)
                f.write("%d,%d,%f\n"%(edge))
        else:
            for edge in edgeList:
                f.write("%d,%d\n"%(edge))
    if(g.vs.attributes()):
        dfNodes = pd.DataFrame()
        dfNodes.index.name='index'
        for attribute in g.vs.attributes():
            dfNodes[attribute] = g.vs[attribute]
        dfNodes.to_csv(nodeCSVPath)

    if(g.es.attributes()):
        dfEdges = pd.DataFrame()
        dfEdges.index.name='index'
        for attribute in g.es.attributes():
            dfEdges[attribute] = g.es[attribute]
        dfEdges.to_csv(edgeCSVPath)
    


