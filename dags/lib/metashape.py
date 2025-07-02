import Metashape


# activate license
def activate(license_string: str):
    """
    Activate the Metashape license.

    Args:
        license_string (str): The license key to activate.
    """
    Metashape.license.activate(license_string)
    print("License activated successfully.")

def get_document(path: str):
    """
    Get the Metashape document from the specified path.
    :param path: The path to the Metashape project file.
    :type path: str
    :return: The Metashape document object.
    """
    doc = Metashape.Document()
    doc.open(path)
    return doc

def create_project(psx_file_name:str):
    """
    Create a new Metashape project.
    :param psx_file_name: The name of the Metashape project file to create.
    :type psx_file_name: str

    :param psx_file_name:
    :return:
    """
    doc = Metashape.Document()

    # create metashape projection location and save project

    metashape_project = psx_file_name
    doc.save(metashape_project)

    # doc.open(metashape_project)
    return doc


def add_photos(chunk: Metashape.Chunk, filenames: list):
    """
    Add photos to the specified chunk.
    :param chunk: The Metashape chunk object.
    :param filenames: A list of filenames to add to the chunk.
    :type filenames: list
    """
    chunk.addPhotos(filenames=filenames)
    print(f"Photos added to chunk '{chunk.label}' successfully.")


def create_chunk(doc: Metashape.Document, label: str):
    """
    Create a new chunk in the Metashape project.
    :param doc: The Metashape document object.
    :param label: The label for the new chunk.
    :type label: str
    """
    chunk = doc.addChunk()
    chunk.label = label
    chunk.addPhotos(filenames=['/Users/hgo/projects/autospex-geowf-python/test_data/DJI_20240925113521_0002.JPG'])
    doc.save()  # Save the project after creating the chunk
    print(f"Chunk '{label}' created successfully.")

def get_chunk(doc:Metashape.Document, label: str):
    """
    Get a chunk by its label.
    :param label: The label of the chunk to retrieve.
    :param doc: The Metashape document object.
    :type label: str
    :return: The chunk object if found, None otherwise.
    """
    for chunk in doc.chunks:
        if chunk.label == label:
            return chunk
    return None


def print_hi():
    return print("hi from metashape.py")
