from pathlib import Path

import click
import os
import pandas as pd
from influxdb_client import InfluxDBClient
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

ALL_PLUGINS = ["proxmox", "pdu", "scaphandre", "k8s", "kepler"]

def get_inventory_ids(url, token, org, bucket, since, vm_name_filter):
    """Query Proxmox data to extract unique inventory-server-id values."""
    flux_query = f'''
    from(bucket: "{bucket}")
      |> range(start: {since})
      |> filter(fn: (r) =>
        r.vm_name =~ /{vm_name_filter}/ and
        r.plugin == "proxmox")
    '''

    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()
    inventory_ids = set()

    try:
        result = query_api.query(flux_query)
        for table in result:
            for record in table.records:
                inv_id = record.values.get("inventory-server-id")
                if inv_id and inv_id.strip():
                    inventory_ids.add(inv_id.strip())
    except Exception as e:
        click.echo(f"⚠️ Error while retrieving inventory IDs: {e}")
    finally:
        client.close()

    return sorted(inventory_ids)

def run_query(url, token, org, bucket, since, plugin, field=None, inventory_id=None, vm_name_filter=None,
              url_match=None, output_dir="data"):
    """Run a query for one inventory ID and save to file."""
    flux_query = f'''
    from(bucket: "{bucket}")
        |> range(start: {since})
        |> filter(fn: (r) => r.plugin == "{plugin}")
    '''
    if field:
        flux_query += f'''  |> filter(fn: (r) => r._field == "{field}")\n'''

    if vm_name_filter and plugin not in ["pdu", "k8s", "kepler"]:
        flux_query += f'''  |> filter(fn: (r) => r.vm_name =~ /{vm_name_filter}/)\n'''

    if inventory_id and plugin != "scaphandre":
        flux_query += f'''  |> filter(fn: (r) => r["inventory-server-id"] == "{inventory_id}")\n'''
    elif not inventory_id and plugin in ["k8s", "kepler"]:
        inventory_id = "Neuronet"
        flux_query += f'''  |> filter(fn: (r) => r["inventory-server-id"] =~ /{inventory_id}/)\n'''

    if url_match and field:
        flux_query += f'''  |> filter(fn: (r) => r.url =~ /{url_match}/)\n'''

    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()

    click.echo(f"🔄 Running query: {flux_query.strip()}")

    try:
        result = query_api.query(flux_query)
        records = []
        for table in result:
            for record in table.records:
                records.append(record.values)

        df = pd.DataFrame(records)
        if df.empty:
            click.echo(f"⚠️ No data for {inventory_id}")
            return

        os.makedirs(f"{output_dir}", exist_ok=True)
        name = inventory_id or vm_name_filter[1:-1]
        output_path = Path(output_dir)
        filename = output_path / f"{plugin}_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        click.echo(f"✅ Saved data for {name} to {filename}")

    except Exception as e:
        click.echo(f"❌ Error querying for {inventory_id}: {e}")

    finally:
        client.close()

def run_plugin(plugin, field=None, url=None, token=None, org=None, bucket=None, since=None, vm_name_filter=None,
               output_dir="data"):
    # First extract inventory IDs using Proxmox
    inventory_ids = get_inventory_ids(url, token, org, bucket, since, vm_name_filter)
    if not inventory_ids:
        click.echo("❌ No inventory-server-id found.")
        return

    if (plugin == "scaphandre" and field) or plugin == "pdu":
        for inv_id in inventory_ids:
            run_query(
                url=url,
                token=token,
                org=org,
                bucket=bucket,
                since=since,
                plugin=plugin,
                field=field,
                inventory_id=inv_id,
                url_match=inv_id,
                output_dir=output_dir
            )
    else:
        run_query(
            url=url,
            token=token,
            org=org,
            bucket=bucket,
            since=since,
            plugin=plugin,
            field=field,
            vm_name_filter=vm_name_filter,
            output_dir=output_dir
        )


@click.command()
@click.option('--url', default='http://10.255.40.16:8086', show_default=True, help='InfluxDB server URL (e.g., http://localhost:8086)')
@click.option('--token', default=None, show_default=True, help='Authorization token for InfluxDB')
@click.option('--org', default='nextworks', show_default=True, help='InfluxDB organization name')
@click.option('--bucket', default='monitoring', show_default=True, help='Bucket name')
@click.option('--since', default='-10m', show_default=True, help='Time range (e.g., -10m, -1h, -1d)')
@click.option('--plugin', required=True, help='Plugin to filter by (e.g., proxmox, pdu, scaphandre) Use "all" to run all plugins.')
@click.option('--vm-name-filter', default='^neuronet-', show_default=True, help='Optional regex filter for vm_name (e.g., ^neuronet-)')
@click.option('--output-dir', default='data', show_default=True, help='Output directory to save output files (default: data)')
def main(url, token, org, bucket, since, plugin, vm_name_filter, output_dir):
    """
    Query InfluxDB for data based on specified parameters and save results to CSV files.

    The following plugins are supported:
    - proxmox: Queries Proxmox data. e.g., "query-influxdb --plugin proxmox"
    - pdu: Queries PDU data. e.g., "query-influxdb --plugin pdu"
    - scaphandre (VMs): Queries Scaphandre data. e.g., "query-influxdb --plugin scaphandre"
    - scaphandre (Host): Queries Scaphandre PDU data. e.g., "query-influxdb --plugin scaphandre --field scaph_host_power_microwatts"
    - k8s: Queries Kubernetes data. e.g., "query-influxdb --plugin k8s"
    - kepler: Queries Kepler data. e.g., "query-influxdb --plugin kepler"
    """

    token = token or os.getenv('INFLUXDB_TOKEN')

    plugins_to_run = ALL_PLUGINS if plugin == "all" else [plugin]

    for plg in plugins_to_run:
        click.echo(f"🔄 Running queries for plugin: {plg}")
        if plg == "scaphandre":
            run_plugin(plg, field=None, url=url, token=token, org=org, bucket=bucket, since=since,
                       vm_name_filter=vm_name_filter, output_dir=output_dir)
            run_plugin(plg, field="scaph_host_power_microwatts", url=url, token=token, org=org, bucket=bucket,
                       since=since, vm_name_filter=vm_name_filter, output_dir=output_dir)
        else:
            run_plugin(plg, field=None, url=url, token=token, org=org, bucket=bucket, since=since,
                       vm_name_filter=vm_name_filter, output_dir=output_dir)


if __name__ == '__main__':
    main()