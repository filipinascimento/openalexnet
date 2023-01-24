from tqdm.auto import tqdm
import openalexnet as oanet
from pathlib import Path

if __name__ == "__main__":

    openalex = oanet.OpenAlexAPI() # add your email to accelerate the API calls. See https://openalex.org/api

    entityType = "works"

    # Entries can be aggregated using the OR operator (up to 50 terms)
    filterData1 = {
        "author.id": "A2394749673|A2122189410", # Mark Newman OR Santo Fortunato
        "is_paratext": "false",  # Only works, no paratexts (https://en.wikipedia.org/wiki/Paratext)
        "type": "journal-article", # Only journal articles
    }

    entities1 = tqdm(openalex.getEntities(entityType, filter=filterData1),desc="Retrieving query 1")

    filterData2 = {
        "author.id": "A2195478976", # Albert-Laszlo Barabasi
        "is_paratext": "false",  # Only works, no paratexts (https://en.wikipedia.org/wiki/Paratext)
        "type": "journal-article", # Only journal articles
    }

    entities2 = tqdm(openalex.getEntities(entityType, filter=filterData2),desc="Retrieving query 2")


    # They can also be aggregated using oanet.aggregateEntities
    aggregatedEntities = oanet.aggregateEntities([entities1,entities2])

    networks = oanet.createNetworks(aggregatedEntities,
                                    networkTypes=["coauthorship","citation"],
                                    simplifyNetworks=True,
                                    showProgress=True)

    # Save the networks to GML files
    networks["citation"].write_gml("citation_network.gml")
    networks["coauthorship"].write_gml("coauthorship_network.gml")

    # Save the networks to edge list files
    oanet.saveNetworkEdgesCSV(networks["citation"],"citation_network.edgelist")
    oanet.saveNetworkEdgesCSV(networks["coauthorship"],"coauthorship_network.edgelist")
