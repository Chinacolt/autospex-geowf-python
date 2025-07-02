from enum import Enum

ACCURACY_LEVELS = {
    "highest": 0,
    "high": 1,
    "medium": 2,
    "low": 4,
    "lowest": 8
}

def get_accuracy_value(accuracy_str):
    key = accuracy_str.lower().strip()
    return ACCURACY_LEVELS.get(key, None)

class TaskName(Enum):
    COPY_DATA_TO_NAS = "copy_data_to_nas"
    CREATE_METASHAPE_PROJECT = "create_metashape_project"
    CREATE_DATA_CHUNKS_HL_HR = "create_data_chunks"
    DISABLE_IMAGES = "disable_images"
    DISABLE_GEO_TAGS = "disable_geo_tags"
    RUN_ALIGNMENT = "run_alignment"
    WAIT_WF_RUN_ALIGNMENT = "run_alignment"
    ENABLE_GEO_TAGS = "enable_geo_tags"
    RUN_TIE_POINT_REMOVAL_REITERATION = "run_tie_point_removal_reiteration"
    DUPLICATE_CHUNK_BLOCK_WITH_HL = "duplicate_chunk_block_with_hl"
    DUPLICATE_CHUNK_BLOCK_WITH_HR = "duplicate_chunk_block_with_hr"
    EXPORT_CAMERA_POSITIONS_HIGH_LEVEL = "export_camera_positions_high_level"
    EXPORT_CAMERA_POSITIONS_HIGH_RESOLUTION = "export_camera_positions_high_resolution"
    # BUILD_3D_MESH_HIGH_LEVEL = "build_3d_mesh_high_level"
