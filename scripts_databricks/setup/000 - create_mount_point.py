# Databricks notebook source
def mount_container(container_name, storage_account_name, access_key, mount_point):
    configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key
    }
    
    mount_source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
    
    try:
        dbutils.fs.mount(
            source=mount_source,
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"Montado com sucesso: {mount_point}")
    except Exception as e:
        print(f"Erro ao montar {mount_point}: {str(e)}")

storage_account_name = "adlstccpucmgengdados"
access_key = "0WOuk6drsxz0Xp4lr6gg5LjHbS0EJ6JsnRR4nxZpcddEHevL9DJyIUbx3As/UqGUvJP4rSu50BZO+AStt+1ycA=="

mount_container("bronze", storage_account_name, access_key, "/mnt/bronze")
mount_container("silver", storage_account_name, access_key, "/mnt/silver")
mount_container("gold", storage_account_name, access_key, "/mnt/gold")