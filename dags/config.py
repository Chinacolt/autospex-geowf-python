import os

class _Config:
    def __init__(self):
        # self.METASHAPE_LICENSE = os.getenv('METASHAPE_LICENSE')
        self.NAS_ROOT = "/nas_root"

print("Config loaded with environment variables:")
print(f"METASHAPE_LICENSE: {os.getenv('METASHAPE_LICENSE')}")
Config = _Config()
