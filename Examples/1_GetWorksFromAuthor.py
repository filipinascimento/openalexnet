from tqdm.auto import tqdm
import openalexnet as oanet
import json

if __name__ == "__main__":

    filterData = {
        "author.id": "A2420755856", # Eugene H. Stanley
        "is_paratext": "false",  # Only works, no paratexts (https://en.wikipedia.org/wiki/Paratext)
        "type": "journal-article", # Only journal articles
        "from_publication_date": "2000-01-01" # Published after 2000
    }

    entityType = "works"

    openalex = oanet.OpenAlexAPI() # add your email to accelerate the API calls. See https://openalex.org/api

    entities = openalex.getEntities(entityType, filter=filterData)

    # Retrieving all entities showing progress bar via tqdm
    entitiesList = []
    for entity in tqdm(entities,desc="Retrieving entries"):
        entitiesList.append(entity)

    # Saving data as json lines (each line is a json object)
    oanet.saveJSONLines(entitiesList,"works_filtered.jsonl")

