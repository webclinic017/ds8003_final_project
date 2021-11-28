# pip install paramiko

import paramiko


def load_staging_to_hive(file_name,
                         table_name,
                         part_col_name=None,
                         part_col_val=None,
                         staging_folder='yahoo_chart_staging'):
    # Let's just ssh into the sandbox to execute hive commands. For a prod setup we'd probably need something more robust,
    #  but for a demo this works just fine
    ssh = paramiko.SSHClient()

    # This is super not cool, but again, dev env so we'd do it properly in prod
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect('sandbox-hdp.hortonworks.com', username='root', password='hdpsandbox', port=2222)

    if part_col_name and part_col_val:
        load_cmd = f"OVERWRITE INTO TABLE {table_name} partition ({part_col_name}='{part_col_val}')"
    else:
        load_cmd = f"INTO TABLE {table_name}"

    hive_load = f"""hive -e "LOAD DATA INPATH '/tmp/{staging_folder}/{file_name}' {load_cmd}" """

    _, ssh_stdout, ssh_stderr = ssh.exec_command(hive_load)
    # print(f"Running hive load: {hive_load}")
    print(ssh_stdout.readlines())
    print(ssh_stderr.readlines())
