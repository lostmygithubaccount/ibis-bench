import os
import shutil
import multiprocessing


def copy_file(args):
    source_file, dest_file = args
    os.makedirs(os.path.dirname(dest_file), exist_ok=True)
    print(f"copying {source_file} to {dest_file}...")
    shutil.copy2(source_file, dest_file)


def copy_tpc_data(source_path, base_sf, multiples):
    copy_tasks = []
    for multiple in multiples:
        new_sf = base_sf * multiple
        new_n = multiple

        for table in [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        ]:
            source_file = os.path.join(
                source_path, f"sf={base_sf}", "n=1", table, "0000.parquet"
            )

            for i in range(new_n):
                dest_dir = os.path.join(
                    source_path, f"sf={new_sf}", f"n={new_n}", table
                )
                dest_file = os.path.join(dest_dir, f"{i:04d}.parquet")
                copy_tasks.append((source_file, dest_file))

    num_cpus = min(128, multiprocessing.cpu_count())
    with multiprocessing.Pool(num_cpus) as pool:
        pool.map(copy_file, copy_tasks)

    print("Data copying completed successfully.")


source_path = "tpch_data/parquet"
base_sf = 128
multiples = [3, 8, 24, 80]

copy_tpc_data(source_path, base_sf, multiples)
