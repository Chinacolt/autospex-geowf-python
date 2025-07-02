import Metashape

from lib.metashape import activate, create_project, create_chunk, get_chunk, get_document, add_photos


# _main
def main():
    # metashape start network client
    #client = Metashape.NetworkClient()
    #client.connect('192.168.1.49')  # server ip address
    #a = client.batchList()
    #print("a", a)


    # Metashape.license.deactivate("APSSA-LEL4T-OB8T8-E3BJR-ANXUO")
    create_project("/Users/fsk/Downloads/p1.psx")
    # doc = get_document("/Users/hgo/projects/autospex-geowf-python/p1.psx")

    # create_chunk(doc=doc, label="Chunk_HL_HR")
    # a = get_chunk(doc=doc, label="Chunk_HL_HR")
    # add_photos(chunk=a, filenames=['/Users/hgo/projects/autospex-geowf-python/test_data/DJI_20240925113521_0002.JPG'])
    # doc.save()
    # print("a", a)


if __name__ == "__main__":
    main()
