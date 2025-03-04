# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = {"fs.azure.account.key.cityoflondoncrime.blob.core.windows.net": "hXnMffy5t5WjiqWB1nZZpBnjnhrvvz4cGIhXSHnT9/CAAFuhkpg5ZmqvMWtcXTeGxDZHsXjSGdQ6+AStzjkEjg=="})
    
    display(dbutils.fs.mounts())
