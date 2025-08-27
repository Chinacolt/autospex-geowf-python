from time import sleep

from metashape_manager.manager import create_cluster, destroy_cluster


def main():
    create_cluster(uniq_id="test",
                   server_instance_type="t3.large",
                   worker_instance_type="t3.xlarge", worker_count=2
                   )
    sleep(30)
    destroy_cluster("test")


if __name__ == "__main__":
    main()
