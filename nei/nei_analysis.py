import pandas as pd
import zipfile
import os
import logging
import dask.dataframe as dd
import requests

logging.basicConfig(level=logging.INFO)


nei_data_path = os.path.abspath('./NEI/nei_ind_data.csv')


if os.path.exists(nei_data_path):

    logging.info('Reading data from csv')
    # nei_data_dd = dd.read_csv(nei_data_path, dtype={'tribal name': str})
    nei_data = pd.read_csv(nei_data_path)

else:
    logging.info('Reading data from zipfiles; writing nei_ind_data.csv')
    nei_data = pd.DataFrame()

    usecols_ = ['eis_facility_id', 'naics_code',
                'facility_source_type',
                'unit_type', 'unit_description', 'eis_process_id',
                'design_capacity', 'design_capacity_uom',
                'scc', 'process_description',
                'pollutant_code', 'total_emissions', 'emissions_uom',
                'emission_factor', 'ef_numerator_uom', 'ef_denominator_uom']

    usecols = [x.replace('_', ' ') for x in usecols_]

    for f in os.listdir("./NEI/"):
        if '.csv' in f:

            try:
                data = pd.read_csv(
                    os.path.join(os.path.dirname(nei_data_path), f),
                    low_memory=False,
                    usecols=usecols_
                    )

            except ValueError:
                data = pd.read_csv(
                    os.path.join(os.path.dirname(nei_data_path), f),
                    low_memory=False,
                    usecols=usecols
                    )
                data.columns = data.columns.str.replace(' ', '_')

            nei_data = nei_data.append(data, sort=False)

        else:
            continue
            # zip_path = os.path.join(os.path.dirname(nei_data_path), f)
            # with zipfile.ZipFile(zip_path) as zf:
            #     for k in zf.namelist():
            #         logging.info(f'File {k}')
            #         if '.csv' in k:
            #             # zipfile throws NotImplementedError: compression type 9 (deflate64)
            #             # for the point source zip file.                         try:
            #                 with zf.open(k) as kf:
            #                     data = pd.read_csv(kf, low_memory=False)
            #                     data.columns = \
            #                         data.columns.str.replace(' ', '_')

            #             except NotImplementedError:
            #                 continue

            #             else:
            #                 nei_data = nei_data.append(data, sort=False)
            #         else:
            #             continue

    nei_data.to_csv(nei_data_path)

# logging.info(f"NEI data head: {nei_data_dd.columns}")

nei_data_process = nei_data.drop_duplicates(subset=['eis_facility_id', 'eis_process_id'])


def id_scc_processes():
    """
    Create yaml that identifies energy-relevant industrial processes
    from EPA's Source Classification Codes
    """

    base_url = '/sccwebservices/v1'
    # blah = f'/LookupElement/facetName[{n}]'
    names = ['SCC Level One']

    for n in names:
        r = requests.get(base_url+f'/LookupElement/facetName[{n}]')


test_data = nei_data.query('pollutant_code=="SO2"')
test_data 