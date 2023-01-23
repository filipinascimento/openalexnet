# OpenAlex Networks (openalexnet)
OpenAlex Networks is a helper library to process and obtain data from the OpenAlex dataset via API. It also provides functionality to generate citation and coauthorship networks from queries.

## Installation

Install using pip

```bash
pip install openalexnet
```

or from source:
```bash
pip git+https://github.com/filipinascimento/openalexnet.git
```

## Usage

Obtaining works from a specific author:

```python
    filterData = {
        "author.id": "A2420755856", # Eugene H. Stanley
        "is_paratext": "false",  # Only works, no paratexts (https://en.wikipedia.org/wiki/Paratext)
        "type": "journal-article", # Only journal articles
        "from_publication_date": "2000-01-01" # Published after 2000
    }

    entityType = "works"

    openalex = oanet.OpenAlexAPI() # add your email to accelerate the API calls. See https://openalex.org/api

    entities = openalex.getEntities(entityType, filter=filterData)

    entitiesList = []
    for entity in tqdm(entities,desc="Retrieving entries"):
        entitiesList.append(entity)

    # Saving data as json lines (each line is a json object)
    oanet.saveJSONLines(entitiesList,"works_filtered.jsonl")
```

Check `Examples` folder for more examples.

## Coming soon
 - Full API documentation
    - More examples


