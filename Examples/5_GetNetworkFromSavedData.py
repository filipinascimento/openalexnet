from tqdm.auto import tqdm
import openalexnet as oanet
from pathlib import Path

if __name__ == "__main__":

    entities = oanet.entitiesFromJSONLines(Path("Examples")/"data"/"works_search.jsonl")


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
