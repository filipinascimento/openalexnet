from tqdm.auto import tqdm
import openalexnet as oanet

if __name__ == "__main__":

    filterData = {
        "author.id": "A2420755856", # Eugene H. Stanley
        "is_paratext": "false",  # Only works, no paratexts (https://en.wikipedia.org/wiki/Paratext)
        "type": "journal-article", # Only journal articles
    }

    entityType = "works"

    openalex = oanet.OpenAlexAPI() # add your email to accelerate the API calls. See https://openalex.org/api

    entities = openalex.getEntities(entityType, filter=filterData)


    networks = oanet.createNetworks(entities,
                                    networkTypes=["coauthorship","citation"],
                                    simplifyNetworks=True,
                                    showProgress=True)

    # Save the networks to GML files
    networks["citation"].write_gml("citation_network.gml")
    networks["coauthorship"].write_gml("coauthorship_network.gml")

    # Save the networks to edge list files
    oanet.saveNetworkEdgesCSV(networks["citation"],"citation_network.edgelist")
    oanet.saveNetworkEdgesCSV(networks["coauthorship"],"coauthorship_network.edgelist")
