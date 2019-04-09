import subprocess


def transfer_to_hdfs(src, dest):

    cmd = f"hdfs dfs -put -f {src} {dest}"

    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(e.output)
