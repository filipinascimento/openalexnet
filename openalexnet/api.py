import requests
import math
from .utilities import _cursorIterator, _pageIterator, makeAPICall, processOAInput


class OpenAlexAPI():
    def __init__(self, email=None):
        self.email = email
        self.session = requests.Session()

    def getEntities(self, entityType, filter={}, search="", sort=[], maxEntities=10000, ignoreEntitiesLimitWarning=False, rateInterval=0.0):
        """Retrieves entities from the OpenAlex API.

        Parameters
        ----------
        entityType : str
            Type of entity to be retrieved from the OpenAlex API. Can be one of the following: "works", "institutions", "authors", "concepts", "venues",.
        filter : dict, list, str optional
            Dictionary of filters to be used in the OpenAlex API call. The keys are the names of the filters and the values are the values of the filters. The default is {}.
            Alternatively, a list of strings parameters formatted as openalex API or a string can be used instead.
        search : str, optional
            Search term to be used in the OpenAlex API call. The default is "".
        sort : list, str, optional
            List of sort terms to be used in the OpenAlex API call. Include ":desc" to the name to sort in descending order. The default is [].
            Alternatively, a string can be used instead.
        maxEntities : int, optional
            Maximum number of entities to be retrieved. If the number of entities in OpenAlex is larger than this number, only the first maxEntities entities will be returned. If maxEntities is set to -1, all entities will be returned. The default is 10000.
        ignoreEntitiesLimitWarning : bool, optional
            If True, the warning that is raised when the number of entities in OpenAlex is larger than maxEntities will be ignored. The default is False.
        rateInterval : float, optional
            Minimum time interval between two consecutive API calls. If the time interval between two consecutive API calls is smaller than rateInterval, the code will wait until the time interval is larger than rateInterval. The default is 0.0.
        Returns
        -------
        iterator
            An iterator of entities retrieved from the OpenAlex API.

        Raises
        ------
        Exception
            If the OpenAlex API call fails, an exception is raised with the error message from the OpenAlex API.

        Examples
        --------
        >>> getEntities("works", {"filter": "institutions.id:https://openalex.org/I33213144,is_paratext:false,type:journal-article,from_publication_date:2022-04-20"})

        """
        parameters = {}
        if (filter):
            if(isinstance(filter, list)):
                parameters["filter"] = ",".join(filter)
            elif(isinstance(filter, dict)):
                parameters["filter"] = processOAInput(filter)
            else:
                parameters["filter"] = filter
        if (search):
            parameters["search"] = search
        if (sort):
            if(isinstance(sort, list)):
                parameters["sort"] = ",".join(sort)
            else:
                parameters["sort"] = sort
        if (self.email):
            parameters["mailto"] = self.email

        parametersFirstCall = {**parameters, "per_page": 200, "page": ""}
        firstResponse = makeAPICall(entityType, parametersFirstCall)
        totalEntries = int(firstResponse["meta"]["count"])
        if (totalEntries > maxEntities and maxEntities >= 0):
            if (not ignoreEntitiesLimitWarning):
                import warnings
                warnings.warn(f"Number of entities ({totalEntries}) in OpenAlex is larger than the maximum allowed ({maxEntities}). Only the first {maxEntities} entities will be returned. You can set the maximum number of entities to be returned by setting maxEntities=yourNumber. To ignore this warning, set ignoreEntitiesLimitWarning=True.",stacklevel=2)
            totalEntries = maxEntities
        totalEntriesPerPage = int(firstResponse["meta"]["per_page"])
        totalPages = math.ceil(totalEntries/totalEntriesPerPage)

        if (totalEntries <= 10000):
            return _pageIterator(entityType, parameters, totalEntries, totalEntriesPerPage, totalPages, rateInterval)
        else:  # using cursor
            return _cursorIterator(entityType, parameters, totalEntries, totalEntriesPerPage, totalPages, rateInterval)


