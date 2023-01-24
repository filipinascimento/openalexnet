import requests
import urllib.parse
import time
import json
import pathlib

k_OPENALEX_API_ENDPOINT = "https://api.openalex.org"

def processOAInput(filterDictionary):
    """Converts a dictionary of filters to a string that can be used in the OpenAlex API call.

    Parameters
    ----------
    filterDictionary : dict
        Dictionary of filters to be used in the OpenAlex API call. The keys are the names of the filters and the values are the values of the filters.

    Returns
    -------
    str
        String that can be used in the OpenAlex API call.
    
    Examples
    --------
    >>> processOAInput({"institutions.id": "https://openalex.org/I33213144", "is_paratext": "false", "type": "journal-article", "from_publication_date": "2022-04-20"})
    'institutions.id:https://openalex.org/I33213144,is_paratext:false,type:journal-article,from_publication_date:2022-04-20'

    """
    inputEntries = []
    for key, value in filterDictionary.items():
        inputEntries.append(f"{key}:{value}")
    return ",".join(inputEntries)

def makeAPICall(entityType, parameters, session=None, rateInterval=0.0):
    """Makes a call to the OpenAlex API.

    Parameters
    ----------
    entityType : str
        Type of entity to be retrieved from the OpenAlex API. Can be one of the following: "works", "institutions", "people", "publications", "funders", "grants", "projects", "datasets", "software", "events", "organizations", "research-outputs", "research-projects", "research-teams", "researchers", "services", "tools", "works".
    parameters : dict
        Dictionary of parameters to be used in the OpenAlex API call. The keys are the names of the parameters and the values are the values of the parameters.
    session : requests.Session (optional)
        Session to be used to make the API call. If not provided, a new session will be created.
    rateInterval : float (optional)
        Time to wait between API calls. Defaults to 0 seconds.
    Returns
    -------
    dict
        Dictionary containing the response from the OpenAlex API.

    Raises
    ------
    Exception
        If the OpenAlex API call fails, an exception is raised with the error message from the OpenAlex API.

    Examples
    --------
    >>> makeAPICall("works", {"filter": "institutions.id:https://openalex.org/I33213144,is_paratext:false,type:journal-article,from_publication_date:2022-04-20"})

    """
    parametersEncoded = urllib.parse.urlencode(parameters)
    requestURL = f"{k_OPENALEX_API_ENDPOINT}/{entityType}?{parametersEncoded}"

    if(rateInterval>0):
        time.sleep(rateInterval)
    if(session is None):
        session = requests
    response = session.get(
        requestURL
    ).json()
    
    if "meta" not in response or "error" in response:
        errorMessage = response
        if("error" in response and "message" in response):
            errorMessage = response["error"]+" -- "+response["message"]
        raise Exception(f"Error in OpenAlex API call for \"{entityType}\":\n\tInput: {parameters}\n\tURL: {requestURL}\n\tResponse: {errorMessage}")
    
    return response

class _pageIterator:
    """
    Iterator that iterates over all the pages of a given entity type and parameters.
    """
    def __init__(self, entityType, parameters, totalEntries,totalEntriesPerPage,totalPages,rateInterval):
        self._entityType = entityType
        self._parameters = parameters.copy()
        self._totalEntries = totalEntries
        self._totalEntriesPerPage = totalEntriesPerPage
        self._totalPages = totalPages
        self._rateInterval = rateInterval

    def __iter__(self):
        self._processedEntries = 0
        for page in range(1,self._totalPages+1):
            allEntries = []
            self._parameters["page"] = page
            self._parameters["per_page"] = self._totalEntriesPerPage
            responsePage = makeAPICall(self._entityType, self._parameters,rateInterval=self._rateInterval)
            shouldBreak = False
            for pageEntry in responsePage["results"]:
                if(self._processedEntries<self._totalEntries):
                    self._processedEntries +=1
                    yield pageEntry
                else:
                    shouldBreak = True
                    break
            if(shouldBreak):
                break
    
    def __len__(self):
        return self._totalEntries


class _cursorIterator:
    """
    Iterator that iterates over all the pages of a given entity type and parameters. Uses cursor instead of pagination.
    """
    def __init__(self, entityType, parameters, totalEntries,totalEntriesPerPage,totalPages,rateInterval):
        self._entityType = entityType
        self._parameters = parameters.copy()
        self._totalEntries = totalEntries
        self._totalEntriesPerPage = totalEntriesPerPage
        self._totalPages = totalPages
        self._rateInterval = rateInterval
        self._parameters["cursor"] = "*"
        self._processedEntries = 0

    def __iter__(self):
        self._parameters["per_page"] = self._totalEntriesPerPage
        while (True):
            responseCursor = makeAPICall(self._entityType, self._parameters,rateInterval=self._rateInterval)
            shouldBreak = False
            for pageEntry in responseCursor["results"]:
                self._processedEntries +=1
                if(self._processedEntries>self._totalEntries):
                    shouldBreak = True
                    break
                yield pageEntry
            if(shouldBreak or "next_cursor" not in responseCursor["meta"] or not responseCursor["meta"]["next_cursor"]):
                break
            self._parameters["cursor"] = responseCursor["meta"]["next_cursor"]
    
    def __len__(self):
        return self._totalEntries


def saveJSONLines(entities,file):
    """Save entities to a JSON Lines file.

    Parameters
    ----------
    entities : iterator
        Iterator of entities to save to the file.
    file : file, str, or pathlib.Path
        File to save the entities to.

    """
    fileHandle = file
    shouldOpenFile = isinstance(file, str) or isinstance(file, pathlib.Path)
    if(shouldOpenFile):
        fileHandle = open(file, "w", encoding="utf-8")

    try:
        for entity in entities:
            fileHandle.write(json.dumps(entity)+"\n")
    finally:
        if(shouldOpenFile):
            fileHandle.close()



def entitiesFromJSONLines(file):
    """Load entities from a JSON Lines file.

    Parameters
    ----------
    file : file, str, or pathlib.Path
        File to load the entities from.

    Returns
    -------
    iterator
        Iterator over the entities in the file.
    """
    fileHandle = file
    shouldOpenFile = isinstance(file, str) or isinstance(file, pathlib.Path)
    if(shouldOpenFile):
        fileHandle = open(file, "r", encoding="utf-8")

    try:
        for line in fileHandle:
            yield json.loads(line)
    finally:
        if(shouldOpenFile):
            fileHandle.close()


def filterDuplicates(entitiesIterator):
    """Filters a list of entities to remove duplicates.

    Parameters
    ----------
    entitiesIterator : iterator
        Iterator over the entities to filter.

    Returns
    -------
    iterator
        Iterator over the entities in the file.
    """
    seenIDs = set()
    for entity in entitiesIterator:
        if(entity["id"] not in seenIDs):
            seenIDs.add(entity["id"])
            yield entity

def aggregateEntities(entitiesIterators):
    """Aggregates a list of iterators over entities into a single iterator over entities.

    Parameters
    ----------
    entitiesIterators : iterator
        Iterator over the iterators over the entities to aggregate. Removes all duplicates.

    Returns
    -------
    iterator
        Iterator over the aggregated entities.
    """
    seenIDs = set()
    for entitiesIterator in entitiesIterators:
        for entity in entitiesIterator:
            if(entity["id"] not in seenIDs):
                seenIDs.add(entity["id"])
                yield entity
