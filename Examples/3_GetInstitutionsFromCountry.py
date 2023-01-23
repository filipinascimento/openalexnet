from tqdm.auto import tqdm
import openalexnet as oanet
import json

if __name__ == "__main__":

    filterData = {
        "country_code":"BR"
    }

    sortData = [
        "works_count:desc"
    ]

    entityType = "institutions"

    openalex = oanet.OpenAlexAPI() # add your email to accelerate the API calls. See https://openalex.org/api

    entities = openalex.getEntities(entityType,
                                            filter=filterData,
                                            sort=sortData,
                                            maxEntities=-1)
    
    # Saving data as json lines (each line is a json object) using iterators only
    oanet.saveJSONLines(tqdm(entities,desc="Retrieving entries"),"institutions_brazil.jsonl")
