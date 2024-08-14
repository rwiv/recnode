import os
import time

start_time = time.time()

directory = "target"
output_file = "out.ts"

file_list = os.listdir(directory)

ts_files = sorted([f for f in file_list if f.endswith(".ts")], key=lambda x: int(x.split(".")[0]))

with open(output_file, "wb") as outfile:
    for ts_file in ts_files:
        with open(os.path.join(directory, ts_file), "rb") as infile:
            outfile.write(infile.read())

end_time = time.time()
print(f"Python merge time: {end_time - start_time} seconds")
