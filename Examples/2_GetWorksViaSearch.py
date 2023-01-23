from tqdm.auto import tqdm
import openalexnet as oanet
import json

if __name__ == "__main__":

    filterData = {
        "is_paratext": "false",  # Only works, no paratexts (https://en.wikipedia.org/wiki/Paratext)
        "type": "journal-article", # Only journal articles
        "from_publication_date": "2020-01-01" # Published after 2020
    }

    searchString = "\"complex network\"" # works containing the string "complex networks"

    sortData = [
        "cited_by_count:desc" # sort by number of citations in descending order
    ]

    entityType = "works"

    openalex = oanet.OpenAlexAPI() # add your email to accelerate the API calls. See https://openalex.org/api


    entities = openalex.getEntities(entityType,
                                            filter=filterData,
                                            search=searchString,
                                            sort=sortData,
                                            maxEntities=1000)
    # maxEntities is optional, it limits the number of entities to retrieve, set it to -1 to get all entities


    # Saving data as json lines (each line is a json object) using iterators only 
    # (no need to store all entities in memory)
    oanet.saveJSONLines(tqdm(entities,desc="Retrieving entries"),"works_search.jsonl")
